package backlogadmin

import (
	"context"
	"sort"
	"time"
)

// DefaultWorkerDownAfter is how long an enrolled worker may be disconnected
// before it counts as down: long enough for a reboot or a restart onto a new
// release, short enough that an outage after a reboot is reported within the
// quarter hour rather than found the next morning.
const DefaultWorkerDownAfter = 10 * time.Minute

// WorkerOutage is one enrolled worker the coordinator cannot reach right now.
//
// It is derived from the workers view, so it says what "worker list" says
// about the same worker: not connected, or connected with a snapshot that is
// too old or from another coordinator epoch.
type WorkerOutage struct {
	WorkerID string `json:"workerId"`
	// LastSeen is when the coordinator last observed the worker, zero when it
	// never has.
	LastSeen time.Time `json:"lastSeen,omitempty"`
	// Since identifies the outage: LastSeen, or the enrollment time of a
	// worker never observed. It does not move while the outage lasts, so it is
	// what deduplicates the notification of one outage.
	Since time.Time `json:"since"`
	// DownFor is how long the outage has lasted as far as this coordinator can
	// tell: from Since, or from when the coordinator started if that is later.
	DownFor        time.Duration `json:"-"`
	DownForSeconds float64       `json:"downForSeconds"`
	// Maintenance marks a worker deliberately out of service: its coordinator
	// configuration sets accept_backlog: false (draining) or its connection is
	// removed. It is reported and never alerted on.
	Maintenance bool `json:"maintenance,omitempty"`
}

// WorkerOutages lists the enrolled workers that are not connected, sorted by
// worker. notBefore is when the observer started watching (the coordinator's
// own start); an outage is never measured from before it.
func WorkerOutages(workers []Worker, now, notBefore time.Time) []WorkerOutage {
	var outages []WorkerOutage
	for _, worker := range workers {
		if worker.Requirement == nil || worker.Enrollment == nil {
			continue
		}
		if worker.Snapshot.Connected && !worker.Stale {
			continue
		}
		outage := WorkerOutage{
			WorkerID:    worker.Requirement.WorkerID,
			LastSeen:    worker.Snapshot.ObservedAt,
			Since:       worker.Snapshot.ObservedAt,
			Maintenance: worker.Requirement.Draining || worker.Requirement.Connection == "removed",
		}
		if outage.Since.IsZero() {
			outage.Since = worker.Enrollment.EnrolledAt
		}
		from := outage.Since
		if from.Before(notBefore) {
			from = notBefore
		}
		outage.DownFor = max(0, now.Sub(from))
		outage.DownForSeconds = outage.DownFor.Seconds()
		outages = append(outages, outage)
	}
	sort.Slice(outages, func(i, j int) bool { return outages[i].WorkerID < outages[j].WorkerID })
	return outages
}

// WorkersDown keeps the outages that have lasted longer than after and are not
// maintenance: the ones that fail "check" and notify the owner. A zero or
// negative after means DefaultWorkerDownAfter.
func WorkersDown(outages []WorkerOutage, after time.Duration) []WorkerOutage {
	if after <= 0 {
		after = DefaultWorkerDownAfter
	}
	var down []WorkerOutage
	for _, outage := range outages {
		if !outage.Maintenance && outage.DownFor > after {
			down = append(down, outage)
		}
	}
	return down
}

// Workers is the workers view for the coordinator's own processes, which hold
// no principal: the owner-notification loop reads it to tell the owner about
// an outage. It returns the time the view was taken as well.
func (s *Service) Workers(ctx context.Context) ([]Worker, time.Time, error) {
	view, err := s.loadView(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	return view.workersResponse(Filter{}), view.now, nil
}
