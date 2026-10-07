package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"time"
)

const coordinatorMigrationV42 = `
CREATE TABLE IF NOT EXISTS coordinator_leases (
 name TEXT PRIMARY KEY,
 record TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS coordinator_lease_receipts (
 request_id TEXT PRIMARY KEY,
 digest TEXT NOT NULL,
 record TEXT NOT NULL
);
`

type leaseReceipt struct {
	Request  domain.LeaseRequest  `json:"request"`
	Response domain.LeaseResponse `json:"response"`
	At       time.Time            `json:"at"`
}

// ExecuteLease runs against the coordinator clock. Read operations neither
// create receipts nor update expiry: an expired row remains a fencing counter.
func (s *Store) ExecuteLease(ctx context.Context, request domain.LeaseRequest) (domain.LeaseResponse, error) {
	if err := request.Validate(); err != nil {
		return domain.LeaseResponse{}, err
	}
	if !request.Mutating() {
		return s.readLease(ctx, request)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return domain.LeaseResponse{}, err
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.LeaseResponse{}, err
	}
	defer tx.Rollback()
	// Take SQLite's write reservation before reading, including on a missing
	// name. This also serializes distinct Store connections to the same database.
	if _, err = tx.ExecContext(ctx, "UPDATE coordinator_leases SET record=record WHERE name=?", request.Name); err != nil {
		return domain.LeaseResponse{}, err
	}
	var priorDigest string
	var prior []byte
	err = tx.QueryRowContext(ctx, "SELECT digest,record FROM coordinator_lease_receipts WHERE request_id=?", request.RequestID).Scan(&priorDigest, &prior)
	if err == nil {
		if priorDigest != digest {
			return domain.LeaseResponse{}, errors.New("lease request id reused with a different digest")
		}
		var receipt leaseReceipt
		if err = json.Unmarshal(prior, &receipt); err != nil {
			return domain.LeaseResponse{}, err
		}
		receipt.Response.Replay = true
		return receipt.Response, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.LeaseResponse{}, err
	}
	lease, err := loadLease(ctx, tx, request.Name)
	if err != nil {
		return domain.LeaseResponse{}, err
	}
	now := s.now().UTC()
	response := domain.LeaseResponse{Lease: lease}
	live := lease != nil && lease.Live(now)
	changed := false
	switch request.Action {
	case "acquire":
		if live {
			if lease.OwnerThread != request.OwnerThread {
				response.Code = 10
				response.Message = heldLeaseMessage(*lease)
			}
		} else {
			var token int64 = 1
			if lease != nil {
				if lease.Token == math.MaxInt64 {
					return domain.LeaseResponse{}, errors.New("lease fencing token exhausted")
				}
				if lease.Token < 0 {
					return domain.LeaseResponse{}, errors.New("invalid stored fencing token")
				}
				token = lease.Token + 1
			}
			lease = &domain.Lease{Name: request.Name, OwnerThread: request.OwnerThread, Principal: request.Principal, Plan: request.Plan, Reason: request.Reason, AcquiredAt: now, ExpiresAt: now.Add(request.EffectiveTTL()), Token: token}
			response.Lease = lease
			changed = true
		}
	case "renew", "release":
		if !live {
			response.Code = 10
			response.Message = "refused: lease is free or expired"
		} else if !request.Force && (lease.OwnerThread != request.OwnerThread || lease.Token != request.Token) {
			response.Code = 10
			response.Message = "refused: holder or fencing token does not match; " + heldLeaseMessage(*lease)
		} else {
			if request.Action == "renew" {
				lease.ExpiresAt = now.Add(request.EffectiveTTL())
			} else {
				lease.Released = true
			}
			changed = true
		}
	}
	if changed {
		raw, err = json.Marshal(lease)
		if err != nil {
			return domain.LeaseResponse{}, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_leases(name,record) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET record=excluded.record", request.Name, raw); err != nil {
			return domain.LeaseResponse{}, err
		}
	}
	raw, err = json.Marshal(leaseReceipt{Request: request, Response: response, At: now})
	if err != nil {
		return domain.LeaseResponse{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_lease_receipts(request_id,digest,record) VALUES(?,?,?)", request.RequestID, digest, raw); err != nil {
		return domain.LeaseResponse{}, err
	}
	if err = tx.Commit(); err != nil {
		return domain.LeaseResponse{}, err
	}
	return response, nil
}

type leaseQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadLease(ctx context.Context, q leaseQuerier, name string) (*domain.Lease, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, "SELECT record FROM coordinator_leases WHERE name=?", name).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var lease domain.Lease
	if err := json.Unmarshal(raw, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

func heldLeaseMessage(lease domain.Lease) string {
	detail := fmt.Sprintf("%q", lease.Reason)
	if lease.Plan != "" {
		detail = fmt.Sprintf("plan %s, %s", lease.Plan, detail)
	}
	return fmt.Sprintf("refused: %s is held by thread %s (%s) until %s", lease.Name, lease.OwnerThread, detail, lease.ExpiresAt.Format(time.RFC3339Nano))
}
func (s *Store) readLease(ctx context.Context, request domain.LeaseRequest) (domain.LeaseResponse, error) {
	response := domain.LeaseResponse{}
	if request.Action == "list" {
		response.Leases = []domain.Lease{}
		rows, err := s.db.QueryContext(ctx, "SELECT record FROM coordinator_leases ORDER BY name")
		if err != nil {
			return response, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var lease domain.Lease
			if err = rows.Scan(&raw); err != nil {
				return response, err
			}
			if err = json.Unmarshal(raw, &lease); err != nil {
				return response, err
			}
			response.Leases = append(response.Leases, lease)
		}
		return response, rows.Err()
	}
	lease, err := loadLease(ctx, s.db, request.Name)
	if err != nil {
		return response, err
	}
	response.Lease = lease
	if lease == nil || !lease.Live(s.now().UTC()) {
		response.Code = 11
		response.Message = "lease is free or expired"
	} else if request.Action == "check" && lease.OwnerThread != request.OwnerThread {
		response.Code = 10
		response.Message = heldLeaseMessage(*lease)
	}
	return response, nil
}
