/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// blockingFactory creates connections like memorytopo, except that a
// connection to blockedCell is only created once release is closed, like
// the etcd2topo factory waits for a cell's topo server that does not answer.
type blockingFactory struct {
	*memorytopo.Factory
	blockedCell string
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

// unblock lets the connections to blockedCell be created.
func (f *blockingFactory) unblock() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *blockingFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	if cell == f.blockedCell {
		select {
		case f.entered <- struct{}{}:
		default:
		}
		<-f.release
	}
	return f.Factory.Create(cell, serverAddr, root)
}

// newBlockingTopo returns a topo server with the cells zone1 and zone2,
// whose connection to zone2 is only created once release is closed.
func newBlockingTopo(t *testing.T) (*topo.Server, *blockingFactory) {
	ctx := t.Context()
	seeded, factory := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	t.Cleanup(seeded.Close)
	bf := &blockingFactory{
		Factory:     factory,
		blockedCell: "zone2",
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	ts, err := topo.NewWithFactory(bf, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	// Cleanups run last first: let the blocked connection be created
	// before Close, so that no ConnForCell still waits for it then.
	t.Cleanup(bf.unblock)
	return ts, bf
}

// TestConnForCellDoesNotWaitForAnotherCell checks that a connection to a
// cell is returned while the connection to another cell, whose topo server
// does not answer, is still being created.
func TestConnForCellDoesNotWaitForAnotherCell(t *testing.T) {
	ts, bf := newBlockingTopo(t)
	ctx := t.Context()

	go func() { _, _ = ts.ConnForCell(ctx, "zone2") }()
	<-bf.entered

	connected := make(chan error, 1)
	go func() {
		_, err := ts.ConnForCell(ctx, "zone1")
		connected <- err
	}()
	select {
	case err := <-connected:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.Fail(t, "ConnForCell(zone1) waited for the connection to zone2")
	}
}

// TestConnForCellWaitForSameCellHonorsContext checks that a caller that waits
// for another caller's connection to the same cell gives up when its context
// ends.
func TestConnForCellWaitForSameCellHonorsContext(t *testing.T) {
	ts, bf := newBlockingTopo(t)

	go func() { _, _ = ts.ConnForCell(t.Context(), "zone2") }()
	<-bf.entered

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	connected := make(chan error, 1)
	go func() {
		_, err := ts.ConnForCell(ctx, "zone2")
		connected <- err
	}()
	select {
	case err := <-connected:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(30 * time.Second):
		require.Fail(t, "ConnForCell(zone2) did not give up when its context ended")
	}
}
