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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
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

// TestConnForCellCloseDoesNotWaitForConnect checks that Close returns while a
// connection to a cell is still being created, and that the connection is
// then closed rather than cached.
func TestConnForCellCloseDoesNotWaitForConnect(t *testing.T) {
	ts, bf := newBlockingTopo(t)

	connected := make(chan error, 1)
	go func() {
		_, err := ts.ConnForCell(t.Context(), "zone2")
		connected <- err
	}()
	<-bf.entered

	closed := make(chan struct{})
	go func() {
		ts.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		require.Fail(t, "Close waited for the connection to zone2")
	}

	bf.unblock()
	select {
	case err := <-connected:
		require.Error(t, err)
		require.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	case <-time.After(30 * time.Second):
		require.Fail(t, "ConnForCell(zone2) did not return after Close")
	}
}

// TestConnForCellWaiterReusesConnection checks that a caller that waits for
// another caller's connection to the same cell gets that connection rather
// than creating another one.
func TestConnForCellWaiterReusesConnection(t *testing.T) {
	ts, bf := newBlockingTopo(t)

	type result struct {
		conn topo.Conn
		err  error
	}
	first := make(chan result, 1)
	go func() {
		conn, err := ts.ConnForCell(t.Context(), "zone2")
		first <- result{conn, err}
	}()
	<-bf.entered

	second := make(chan result, 1)
	go func() {
		conn, err := ts.ConnForCell(t.Context(), "zone2")
		second <- result{conn, err}
	}()
	// Only let the first connection be created once the second caller
	// waits for it.
	assert.Eventually(t, func() bool {
		return goroutinesInSemaphoreAcquire() == 1
	}, 30*time.Second, 10*time.Millisecond)
	bf.unblock()

	r1, r2 := <-first, <-second
	require.NoError(t, r1.err)
	require.NoError(t, r2.err)
	assert.Same(t, r1.conn, r2.conn)
}

// TestConnForCellRepointedCell checks that a cell whose ServerAddress changes
// gets a new connection, which is then cached.
func TestConnForCellRepointedCell(t *testing.T) {
	ts, _ := newBlockingTopo(t)
	ctx := t.Context()

	before, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	err = ts.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = "repointed"
		return nil
	})
	require.NoError(t, err)

	after, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, before, after)

	cached, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	assert.Same(t, after, cached)
}

// goroutinesInSemaphoreAcquire returns how many goroutines wait in
// semaphore.Weighted.Acquire.
func goroutinesInSemaphoreAcquire() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "semaphore.(*Weighted).Acquire")
}
