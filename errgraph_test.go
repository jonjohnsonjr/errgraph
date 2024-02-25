package errgraph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jonjohnsonjr/errgraph"
)

func TestErrgraphCycle(t *testing.T) {
	g := errgraph.New(0)
	ctx := context.Background()

	cerr := &errgraph.CycleError{}
	err := cycle(ctx, g)
	if !errors.As(err, &cerr) {
		t.Fatalf("wanted CycleError, got %T", err)
	}
}

func cycle(ctx context.Context, g *errgraph.Graph) error {
	return g.Do(ctx, "foo", func(ctx context.Context, g *errgraph.Graph) error {
		return g.Do(ctx, "bar", func(ctx context.Context, g *errgraph.Graph) error {
			return g.Do(ctx, "foo", func(ctx context.Context, g *errgraph.Graph) error {
				return fmt.Errorf("did I get here")
			})
		})
	})
}

func TestErrgraphDedupe(t *testing.T) {
	g := errgraph.New(0)
	ctx := context.Background()

	if err := g.Do(ctx, "key", func(ctx context.Context, g *errgraph.Graph) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Do(ctx, "key", func(ctx context.Context, g *errgraph.Graph) error {
		return fmt.Errorf("deduped error should never get returned")
	}); err != nil {
		t.Fatal(err)
	}
}

func TestErrgraphDedupeWaiters(t *testing.T) {
	g := errgraph.New(10)
	ctx := context.Background()

	ch := make(chan struct{})
	for range 100 {
		g.Go(ctx, "key", func(ctx context.Context, g *errgraph.Graph) error {
			<-ch
			return nil
		})
	}

	// Since we have a limit of 10, we will never get here unless we dedupe.
	// This currently costs a goroutine anyway, but we could probably coallesce waiters somehow.
	ch <- struct{}{}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}
