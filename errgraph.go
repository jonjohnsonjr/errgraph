// Package errgraph deduplicates work by a given key for a dag.
package errgraph

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/exp/slices"
)

type Task struct {
	cond *sync.Cond
	done bool
}

func newTask() *Task {
	return &Task{
		cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
}

func (t *Task) Lock() {
	t.cond.L.Lock()
}

func (t *Task) Unlock() {
	t.cond.L.Unlock()
}

type token struct{}

// TODO: Generic key.
// TODO: Results?
// TODO: Separate dedupe from everything else?
type Graph struct {
	// global to the graph
	// string -> *Task
	tasks *sync.Map

	// string -> error
	errors *sync.Map

	sem chan token

	// local to this subgraph
	path []string

	// string -> *Task
	waiting *sync.Map

	errOnce sync.Once
	err     error
}

func (g *Graph) error(key string) error {
	v, ok := g.errors.Load(key)
	if !ok || v == nil {
		return nil
	}
	return v.(error)
}

// New returns a Graph that executes at most limit leaves at a time.
// If limit is <= 0, there is no limit.
// Functions are deduplicated by their key string and will return whatever the first execution returned.
func New(limit int) *Graph {
	g := &Graph{
		tasks:   &sync.Map{},
		errors:  &sync.Map{},
		waiting: &sync.Map{},
		path:    []string{},
	}

	if limit > 0 {
		g.sem = make(chan token, limit)
	}

	return g
}

type CycleError struct {
	key  string
	path []string
}

func (e *CycleError) Error() string {
	return fmt.Sprintf("cannot execute subgraph due to cycle: %s -> %s", strings.Join(e.path, " -> "), e.key)
}

// Do does stuff synchronously.
func (g *Graph) Do(ctx context.Context, key string, f func(context.Context, *Graph) error) error {
	if err := g.cycle(key); err != nil {
		return err
	}
	v, loaded := g.tasks.LoadOrStore(key, newTask())
	task := v.(*Task)
	if loaded {
		task.Lock()
		for !task.done {
			task.cond.Wait()
		}
		task.Unlock()
	} else {
		subgraph := g.subgraph(key)

		g.errors.Store(key, f(ctx, subgraph))

		task.Lock()
		task.done = true
		task.Unlock()

		task.cond.Broadcast()
	}

	return g.error(key)
}

func (g *Graph) subgraph(key string) *Graph {
	subpath := slices.Clone(g.path)
	subpath = append(subpath, key)
	return &Graph{
		waiting: &sync.Map{},
		tasks:   g.tasks,
		errors:  g.errors,
		sem:     g.sem,
		path:    subpath,
	}
}

// TODO: switch to map if path len is too big? benchmark it?
func (g *Graph) cycle(key string) error {
	for _, p := range g.path {
		if p == key {
			return &CycleError{
				key:  key,
				path: g.path,
			}
		}
	}

	return nil
}

// Go does stuff asynchronously (think errgroup).
func (g *Graph) Go(ctx context.Context, key string, f func(context.Context, *Graph) error) {
	if err := g.cycle(key); err != nil {
		g.errOnce.Do(func() {
			g.err = err
		})
		return
	}

	v, loaded := g.tasks.LoadOrStore(key, newTask())
	task := v.(*Task)
	g.waiting.Store(key, task)

	if loaded {
		return
	}

	if g.sem != nil {
		g.sem <- token{}
	}

	go func() {
		err := f(ctx, g.subgraph(key))
		g.errors.Store(key, err)
		g.errOnce.Do(func() {
			g.err = err
		})

		task.Lock()
		task.done = true

		if g.sem != nil {
			<-g.sem
		}

		task.cond.Broadcast()
		task.Unlock()
	}()
}

func (g *Graph) Wait() error {
	g.waiting.Range(func(k, v any) bool {
		task := v.(*Task)

		task.Lock()
		defer task.Unlock()

		for !task.done {
			task.cond.Wait()
		}

		return true
	})

	return g.err
}

// spiking some notes, this one isn't used yet
// need to make sure this makes sense but I wonder if we can "loan" the semaphore one unit
// whenever we enter into a cond.Wait() so that we don't end up waiting indefinitely.
func (g *Graph) wait() error {
	g.waiting.Range(func(k, v any) bool {
		task := v.(*Task)

		// TODO: Would it ever make sense to TryLock?
		task.Lock()
		defer task.Unlock()

		for !task.done {
			if g.sem != nil {
				<-g.sem
			}

			task.cond.Wait()

			if g.sem != nil {
				g.sem <- token{}
			}
		}

		return true
	})

	return g.err
}
