// Package errgraph deduplicates work by a given key for a dag.
package errgraph

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/exp/slices"
)

type token struct{}

type Graph struct {
	// string -> sync.Once
	tasks *sync.Map

	// string -> error
	errors *sync.Map

	sem chan token

	path []string
}

func (g *Graph) err(key string) error {
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
		g.errors.Store(key, f(ctx, subgraph))
	})

	return g.err(key)
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

		g.errors.Store(key, f(ctx))
	})

	return g.err(key)
}
