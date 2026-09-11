package backlog

import "testing"

func FuzzManifestAndScheduleParsers(f *testing.F) {
	f.Add("version: 2\nname: safe\ntasks: {}\n", "0 2 * * *")
	f.Add("---\nunknown: value\n", "*/0 * * * *")
	f.Fuzz(func(t *testing.T, manifest, schedule string) {
		if len(manifest) > 16<<10 {
			manifest = manifest[:16<<10]
		}
		if len(schedule) > 1024 {
			schedule = schedule[:1024]
		}
		_, _ = ParseManifest([]byte(manifest))
		_, _ = ParseScheduleExpression(schedule)
	})
}
