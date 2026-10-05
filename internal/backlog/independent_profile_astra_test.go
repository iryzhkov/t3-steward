package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"testing"
)

func TestIndependentProfileWriterRejectsRehashedOverride(t *testing.T) {
	for _, kind := range []string{"effort", "pool", "turns", "resources", "all-profiles-removed"} {
		t.Run(kind, func(t *testing.T) {
			f := executionFixture(t)
			ctx := context.Background()
			original, err := f.service.ResolveDeclared(ctx, declaredRequest(f))
			if err != nil {
				t.Fatal(err)
			}
			changed, err := original.Authority.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			p := changed.Requirements.Members[0].Execution
			switch kind {
			case "effort":
				p.Effort = "high"
			case "pool":
				p.QuotaPoolID = "other"
			case "turns":
				p.MaxTurns++
			case "resources":
				p.Resources.MemoryMB++
			case "all-profiles-removed":
				for i := range changed.Requirements.Members {
					changed.Requirements.Members[i].Execution = nil
				}
			}
			req, err := review.NewRequirements(changed.Requirements)
			if err != nil {
				t.Fatal(err)
			}
			changed.RequirementsDigest = req.Digest()
			writer, err := sqlite.OpenMigrated(f.store.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			db := stagingSQL(t, f)
			before := independentDeclaredTables(t, db)
			if _, err = writer.FreezeDeclaredReviewAuthority(ctx, changed); err == nil {
				t.Fatal("writer accepted rehashed override")
			}
			if before != independentDeclaredTables(t, db) {
				t.Fatal("refusal mutated logical SQL/native audit")
			}
			if _, err = writer.FreezeDeclaredReviewAuthority(ctx, original.Authority); err != nil {
				t.Fatal("original authority no longer accepted", err)
			}
			before = independentDeclaredTables(t, db)
			if _, err = writer.FreezeDeclaredReviewAuthority(ctx, changed); err == nil {
				t.Fatal("replay accepted replacement")
			}
			if before != independentDeclaredTables(t, db) {
				t.Fatal("replay refusal mutated SQL")
			}
			t.Log("initial writer and frozen replay refused replacement; original accepted")
		})
	}
}
