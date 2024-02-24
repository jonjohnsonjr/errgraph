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

type Graph struct {
	// string -> *Task
	tasks *sync.Map

	// string -> error
	errors *sync.Map

	sem chan token

	path []string

	wg sync.WaitGroup

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
		path: []string{},
	}

	if limit > 0 {
		g.sem = make(chan token, limit)
	}

	return g
}

// Do does stuff.
func (g *Graph) Do(ctx context.Context, key string, f func(context.Context, *Graph) error) error {
	for _, p := range g.path {
		if p == key {
			return fmt.Errorf("cannot execute subgraph due to cycle: %s -> %s", strings.Join(g.path, " -> "), key)
		}
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
		subpath := slices.Clone(g.path)
		subpath = append(subpath, key)
		subgraph := &Graph{
			tasks: g.tasks,
			sem:   g.sem,
			path:  subpath,
		}

		g.errors.Store(key, f(ctx, subgraph))

		task.Lock()
		task.done = true
		task.Unlock()

		task.cond.Broadcast()
	}

	return g.error(key)
}

// Go is like Do but async.
func (g *Graph) Go(ctx context.Context, key string, f func(context.Context, *Graph) error) {
	for _, p := range g.path {
		if p == key {
			g.errOnce.Do(func() {
				g.err = fmt.Errorf("cannot execute subgraph due to cycle: %s -> %s", strings.Join(g.path, " -> "), key)
			})
			return
		}
	}

	v, loaded := g.tasks.LoadOrStore(key, newTask())
	task := v.(*Task)
	if loaded {
		task.Lock()
		if task.done {
			task.Unlock()
			return
		}

		g.wg.Add(1)
		go func() {
			defer g.wg.Done()

			for !task.done {
				task.cond.Wait()
			}
			task.Unlock()
		}()
	} else {
		subpath := slices.Clone(g.path)
		subpath = append(subpath, key)
		subgraph := &Graph{
			tasks: g.tasks,
			sem:   g.sem,
			path:  subpath,
		}

		if g.sem != nil {
			g.sem <- token{}
		}
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()

			err := f(ctx, subgraph)
			g.errors.Store(key, err)
			g.errOnce.Do(func() {
				g.err = err
			})

			task.Lock()
			task.done = true
			task.Unlock()

			<-g.sem

			task.cond.Broadcast()
		}()
	}
}

func (g *Graph) Wait() error {
	g.wg.Wait()
	return g.err
}
