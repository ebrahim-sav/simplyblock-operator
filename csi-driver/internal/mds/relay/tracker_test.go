// Tests for the tracker: it hands the guest a new connection when the control
// plane publishes a volume somewhere else, and does nothing while it does not.

package relay

import (
	"context"
	"errors"
	"testing"

	"github.com/simplyblock/atlas/lvol"
	export "github.com/simplyblock/atlas/nfsexport"
)

// movingResolver answers with conn, which a test changes between calls.
type movingResolver struct {
	conn  lvol.Connection
	err   error
	asked int
}

func (m *movingResolver) resolve(context.Context, export.Spec, string) (lvol.Connection, string, error) {
	m.asked++
	return m.conn, "", m.err
}

func moved(addrs ...string) lvol.Connection {
	c := lvol.Connection{NQN: resolved.NQN, NSID: resolved.NSID, UUID: resolved.UUID}
	for _, a := range addrs {
		c.Endpoints = append(c.Endpoints, lvol.Endpoint{Transport: "tcp", Address: a, Port: 4420})
	}
	return c
}

func newTracker(g *guest, m *movingResolver) *Tracker {
	return &Tracker{Relay: Relay{Guest: g, Resolve: m.resolve}, Log: func(string, ...any) {}}
}

func TestTheTrackerLeavesAnUnmovedVolumeAlone(t *testing.T) {
	g, m := &guest{}, &movingResolver{conn: moved("10.10.1.11", "10.10.1.12")}
	tr := newTracker(g, m)
	ctx := context.Background()
	if err := tr.Create(ctx, spec()); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The same endpoints in another order are the same paths.
	m.conn = moved("10.10.1.12", "10.10.1.11")
	tr.Refresh(ctx)
	if len(g.created) != 1 {
		t.Errorf("the guest got %d Creates, want only the operator's", len(g.created))
	}
	if m.asked != 2 {
		t.Errorf("the control plane was asked %d times, want once per Create and once per refresh", m.asked)
	}
}

func TestTheTrackerReassemblesAVolumePublishedElsewhere(t *testing.T) {
	g, m := &guest{}, &movingResolver{conn: moved("10.10.1.11", "10.10.1.12")}
	tr := newTracker(g, m)
	ctx := context.Background()
	_ = tr.Create(ctx, spec())

	m.conn = moved("10.10.1.13", "10.10.1.12")
	tr.Refresh(ctx)
	if len(g.created) != 2 {
		t.Fatalf("the guest got %d Creates, want a second one with the new endpoints", len(g.created))
	}
	got := g.created[1].Connection
	if got == nil || len(got.Endpoints) != 2 || got.Endpoints[0].Address != "10.10.1.13" {
		t.Errorf("the second Create carried %+v, want the endpoints the control plane publishes now", got)
	}

	tr.Refresh(ctx)
	if len(g.created) != 2 {
		t.Errorf("the guest got %d Creates, want none once it has the new endpoints", len(g.created))
	}
}

func TestTheTrackerForgetsADeletedExport(t *testing.T) {
	g, m := &guest{}, &movingResolver{conn: moved("10.10.1.11")}
	tr := newTracker(g, m)
	ctx := context.Background()
	_ = tr.Create(ctx, spec())
	if err := tr.Delete(ctx, spec()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	m.conn = moved("10.10.1.13")
	asked := m.asked
	tr.Refresh(ctx)
	if len(g.created) != 1 || m.asked != asked {
		t.Errorf("a deleted export was refreshed: %d Creates, %d more questions", len(g.created), m.asked-asked)
	}
}

func TestTheTrackerKeepsTheGuestAsItIsWhenTheControlPlaneDoesNotAnswer(t *testing.T) {
	g, m := &guest{}, &movingResolver{conn: moved("10.10.1.11")}
	tr := newTracker(g, m)
	ctx := context.Background()
	_ = tr.Create(ctx, spec())
	m.err = errors.New("control plane unreachable")
	m.conn = lvol.Connection{}
	tr.Refresh(ctx)
	if len(g.created) != 1 {
		t.Errorf("the guest got %d Creates on an unanswered question", len(g.created))
	}
}

func TestTheTrackerDoesNotTrackACreateThatFailed(t *testing.T) {
	g, m := &guest{}, &movingResolver{err: errors.New("control plane unreachable")}
	tr := newTracker(g, m)
	ctx := context.Background()
	if err := tr.Create(ctx, spec()); err == nil {
		t.Fatal("Create succeeded without a connection")
	}
	m.err = nil
	m.conn = moved("10.10.1.11")
	tr.Refresh(ctx)
	if len(g.created) != 0 {
		t.Errorf("the guest got %d Creates for an export that never assembled", len(g.created))
	}
}
