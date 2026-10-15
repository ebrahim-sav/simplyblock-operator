// The tracker: it keeps the guest's exports on the endpoints the control plane
// publishes now, by asking again periodically and reassembling an export whose
// volume is published somewhere else (design-pnfs-mds-vm.md §7.6).
//
// The guest reconnects missing paths by itself, from the connection its last
// Create carried, but it cannot ask the control plane, so a volume that moved
// after a migration or a failover would keep its old endpoints until the next
// grow or restart. The runner can ask, and does it here.

package relay

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/simplyblock/atlas/lvol"
	export "github.com/simplyblock/atlas/nfsexport"
)

// DefaultTrackInterval is how often the tracker asks the control plane where
// each export's volume is published. A volume moves rarely, and each question
// is one control-plane call per export.
const DefaultTrackInterval = 5 * time.Minute

// Tracker is a Relay that remembers what it assembled.
type Tracker struct {
	Relay Relay
	// Log reports each reassembly and each failed refresh.
	Log func(format string, args ...any)

	mu      sync.Mutex
	exports map[string]tracked
}

type tracked struct {
	spec export.Spec
	conn lvol.Connection
}

// Create assembles the export and remembers the connection it was handed.
func (t *Tracker) Create(ctx context.Context, spec export.Spec) error {
	conn, err := t.Relay.create(ctx, spec)
	if err != nil {
		return err
	}
	t.track(spec, conn)
	return nil
}

// Delete tears the export down and forgets it.
func (t *Tracker) Delete(ctx context.Context, spec export.Spec) error {
	if err := t.Relay.Delete(ctx, spec); err != nil {
		return err
	}
	t.mu.Lock()
	delete(t.exports, spec.Path)
	t.mu.Unlock()
	return nil
}

// Check is the relay's.
func (t *Tracker) Check(ctx context.Context, spec export.Spec) error {
	return t.Relay.Check(ctx, spec)
}

func (t *Tracker) track(spec export.Spec, conn lvol.Connection) {
	spec.Connection = nil
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.exports == nil {
		t.exports = map[string]tracked{}
	}
	t.exports[spec.Path] = tracked{spec: spec, conn: conn}
}

// Refresh asks the control plane where every tracked export's volume is
// published, and reassembles the ones published somewhere else.
func (t *Tracker) Refresh(ctx context.Context) {
	t.mu.Lock()
	all := make([]tracked, 0, len(t.exports))
	for _, e := range t.exports {
		all = append(all, e)
	}
	t.mu.Unlock()
	slices.SortFunc(all, func(a, b tracked) int { return strings.Compare(a.spec.Path, b.spec.Path) })

	for _, e := range all {
		if ctx.Err() != nil {
			return
		}
		now, _, err := t.Relay.Resolve(ctx, e.spec, e.spec.HostNQN)
		if err != nil {
			t.logf("export %s: asking where volume %s is published: %v", e.spec.Path, e.spec.VolumeUUID, err)
			continue
		}
		if samePaths(now, e.conn) {
			continue
		}
		t.logf("export %s: volume %s is published at %v, not %v; reassembling",
			e.spec.Path, e.spec.VolumeUUID, paths(now), paths(e.conn))
		if !t.stillTracked(e.spec.Path) {
			continue
		}
		if err := t.Create(ctx, e.spec); err != nil {
			t.logf("export %s: reassembling: %v", e.spec.Path, err)
		}
	}
}

func (t *Tracker) stillTracked(path string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.exports[path]
	return ok
}

// Run refreshes every interval until ctx ends.
func (t *Tracker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.Refresh(ctx)
		}
	}
}

func (t *Tracker) logf(format string, args ...any) {
	if t.Log != nil {
		t.Log(format, args...)
	}
}

// samePaths reports whether two connections reach the same namespace over the
// same endpoints, in any order: the order is attach priority, and the guest's
// paths are the same set whichever comes first.
func samePaths(a, b lvol.Connection) bool {
	return a.NQN == b.NQN && a.NSID == b.NSID && slices.Equal(paths(a), paths(b))
}

// paths is a connection's endpoints as sorted transport/address:port strings.
func paths(c lvol.Connection) []string {
	out := make([]string, 0, len(c.Endpoints))
	for _, e := range c.Endpoints {
		out = append(out, e.Transport+"/"+e.Address+":"+strconv.Itoa(e.Port))
	}
	slices.Sort(out)
	return out
}
