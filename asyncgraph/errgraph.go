// Package asyncgraph deduplicates work by a given key for a dag.
package asyncgraph

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type token struct{}

type Graph struct {
	// string -> sync.Once
	tasks *sync.Map

	deps *errgroup.Group

	sem chan token

	path []string
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

// Sub creates a subgraph from Graph and executes f.
// Sub functions are not governed by the Graph's limit, so they should avoid doing heavy work.
// For heavy work, use Leaf.
func (g *Graph) Sub(ctx context.Context, key string, f func(context.Context, *Graph) error) error {
	for _, p := range g.path {
		if p == key {
			return fmt.Errorf("cannot execute subgraph due to cycle: %s -> %s", strings.Join(g.path, " -> "), key)
		}
	}

	once, _ := g.tasks.LoadOrStore(key, &sync.Once{})
	once.(*sync.Once).Do(func() {
		subpath := slices.Clone(g.path)
		subpath = append(subpath, key)
		subgraph := &Graph{
			tasks: g.tasks,
			sem:   g.sem,
			path:  subpath,
		}
		g.deps.Go(func() error {
			return f(ctx, subgraph)
		})
	})

	return nil
}

// Leaf executes f, governed by the Graph's limit.
func (g *Graph) Leaf(ctx context.Context, key string, f func(context.Context) error) error {
	for _, p := range g.path {
		if p == key {
			return fmt.Errorf("cannot execute leaf due to cycle: %s -> %s", strings.Join(g.path, " -> "), key)
		}
	}

	once, _ := g.tasks.LoadOrStore(key, &sync.Once{})
	once.(*sync.Once).Do(func() {
		if g.sem != nil {
			g.sem <- token{}
		}

		g.deps.Go(func() error {
			return f(ctx)
		})
	})

	return nil
}

// Wait blocks until all function calls from the Sub and Leaf methods have returned,
// then returns the first non-nil error (if any) from them.
func (g *Graph) Wait() error {
	return g.deps.Wait()
}
