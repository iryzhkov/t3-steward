package jocasta

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCLI writes a shell script that behaves like the jocasta CLI for the
// three commands the ledger uses: documents live as files under dir/docs, each
// with a .rev file holding its revision. Creating dir/unavailable makes every
// command fail the way an unreachable service does.
func fakeCLI(t *testing.T) (binary, dir string) {
	t.Helper()
	dir = t.TempDir()
	script := `#!/bin/sh
D='` + dir + `'
cmd=$1; shift
path=$1; shift
file=""; out=""; create=0; ifrev=""; reqid=""
while [ $# -gt 0 ]; do
  case "$1" in
    --file) file=$2; shift 2;;
    --output) out=$2; shift 2;;
    --if-revision) ifrev=$2; shift 2;;
    --request-id) reqid=$2; shift 2;;
    --create) create=1; shift;;
    --overwrite|--json) shift;;
    *) echo "unexpected argument $1" >&2; exit 6;;
  esac
done
echo "$cmd $path create=$create if=$ifrev request=$reqid" >> "$D/calls"
if [ -f "$D/unavailable" ]; then
  echo '{"version":"jocasta/v1","error":{"code":"unavailable","message":"service unreachable"}}'; exit 5
fi
doc="$D/docs/$path"
receipt() { printf '{"version":"jocasta/v1","document":{"id":"doc1","path":"%s","revision":%s}}\n' "$path" "$1"; }
notfound() { echo '{"version":"jocasta/v1","error":{"code":"not_found","message":"document or revision does not exist"}}'; exit 2; }
conflict() { echo '{"version":"jocasta/v1","error":{"code":"conflict","message":"guard did not hold"}}'; exit 3; }
case "$cmd" in
get)
  [ -f "$doc" ] || notfound
  cp "$doc" "$out"; receipt "$(cat "$doc.rev")";;
put)
  if [ $create = 1 ]; then
    [ -f "$doc" ] && conflict
    mkdir -p "$(dirname "$doc")"; cp "$file" "$doc"; echo 1 > "$doc.rev"; receipt 1
  else
    [ -f "$doc" ] || notfound
    cur=$(cat "$doc.rev")
    [ "$cur" = "$ifrev" ] || conflict
    cp "$file" "$doc"; next=$((cur+1)); echo $next > "$doc.rev"; receipt $next
  fi;;
*) exit 6;;
esac
`
	binary = filepath.Join(dir, "jocasta")
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return binary, dir
}

func TestCLICreateGetUpdateAndConflict(t *testing.T) {
	binary, dir := fakeCLI(t)
	temp := t.TempDir()
	cli := CLI{Binary: binary, TempDir: temp}
	ctx := context.Background()
	const path = "steward/handoffs/run-1.md"

	revision, err := cli.Create(ctx, path, []byte("# ledger\n"), "key-open")
	if err != nil || revision != 1 {
		t.Fatalf("create = %d, %v", revision, err)
	}
	if _, err := cli.Create(ctx, path, []byte("other"), "key-other"); !IsConflict(err) {
		t.Fatalf("create over an existing path = %v, want conflict", err)
	}
	doc, err := cli.Get(ctx, path)
	if err != nil || doc.Revision != 1 || string(doc.Content) != "# ledger\n" || doc.Path != path {
		t.Fatalf("get = %#v, %v", doc, err)
	}
	revision, err = cli.Update(ctx, path, []byte("# ledger\nrecord\n"), 1, "key-append")
	if err != nil || revision != 2 {
		t.Fatalf("update = %d, %v", revision, err)
	}
	if _, err := cli.Update(ctx, path, []byte("stale"), 1, "key-stale"); !IsConflict(err) {
		t.Fatalf("stale update = %v, want conflict", err)
	}
	if _, err := cli.Get(ctx, "steward/handoffs/missing.md"); CodeOf(err) != CodeNotFound {
		t.Fatalf("missing get = %v, want not_found", err)
	}

	calls, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"put " + path + " create=1 if= request=key-open",
		"put " + path + " create=0 if=1 request=key-append",
	} {
		if !strings.Contains(string(calls), want) {
			t.Fatalf("calls do not contain %q:\n%s", want, calls)
		}
	}
	// Content passes through private temporary files that never outlive a call.
	if entries, err := os.ReadDir(temp); err != nil || len(entries) != 0 {
		t.Fatalf("temporary files left behind: %v %v", entries, err)
	}
}

func TestCLIUnavailableAndMissingBinary(t *testing.T) {
	binary, dir := fakeCLI(t)
	if err := os.WriteFile(filepath.Join(dir, "unavailable"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cli := CLI{Binary: binary, TempDir: t.TempDir()}
	_, err := cli.Get(context.Background(), "steward/handoffs/run-1.md")
	if CodeOf(err) != CodeUnavailable || IsConflict(err) {
		t.Fatalf("unavailable get = %v (%q)", err, CodeOf(err))
	}

	missing := CLI{Binary: filepath.Join(t.TempDir(), "no-such-jocasta"), TempDir: t.TempDir()}
	_, err = missing.Create(context.Background(), "steward/handoffs/run-1.md", []byte("x"), "key")
	if CodeOf(err) != CodeMissingCLI {
		t.Fatalf("missing CLI = %v (%q)", err, CodeOf(err))
	}
}

func TestErrorCodeFallsBackToExitStatus(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "jocasta")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\necho not json\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cli := CLI{Binary: binary, TempDir: t.TempDir()}
	_, err := cli.Update(context.Background(), "steward/handoffs/run-1.md", []byte("x"), 4, "key")
	if !IsConflict(err) {
		t.Fatalf("exit 3 without JSON = %v, want conflict", err)
	}
}
