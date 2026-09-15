#!/usr/bin/env python3
"""Execute the actual generated unit as a disposable SSH-config ownership probe."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

PROBE = r'''#!/usr/bin/python3
import json, os, pathlib, subprocess, sys
assert sys.argv[1:3] == ["run", "--config"]
output = pathlib.Path(sys.argv[3])
command = ["/usr/bin/ssh", "-G", "-F", "/etc/ssh/ssh_config",
           "-o", "CanonicalizeHostname=no", "-o", "ProxyCommand=none",
           "-o", "HostName=127.0.0.1", "t3-unit-qualification.invalid"]
result = subprocess.run(command, capture_output=True, text=True, timeout=15)
status = pathlib.Path("/proc/self/status").read_text()
record = {"command": command, "sshExit": result.returncode,
          "stdout": result.stdout, "stderr": result.stderr,
          "uid": os.getuid(), "sshConfigUID": os.stat("/etc/ssh/ssh_config").st_uid,
          "uidMap": pathlib.Path("/proc/self/uid_map").read_text(),
          "noNewPrivileges": next(line for line in status.splitlines() if line.startswith("NoNewPrivs:"))}
output.write_text(json.dumps(record, indent=2) + "\n")
# The SSH exit is evidence, not our exit: preserve generated Restart=on-failure
# without making an expected SSH refusal restart this disposable probe forever.
'''

RENDER = '''package main
import ("fmt";"os";"github.com/iryzhkov/t3-steward/internal/platform")
func main(){fmt.Print(platform.RenderUnit(platform.InstallOptions{Binary:os.Args[1],ConfigPath:os.Args[2]}))}
'''

def command(args, **options):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=45, **options)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--expect-ssh", type=int, choices=[0, 255], required=True)
    parser.add_argument("--expect-protection", choices=["full", "no"], required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    root = Path(tempfile.mkdtemp(prefix="t3-unit-ssh."))
    unit_name = "t3-qualification-ssh-" + root.name.split(".")[-1] + ".service"
    print("evidenceRoot=" + str(root), flush=True)
    source_commit = command(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip()
    source_status = command(["git", "-C", str(source), "status", "--short"]).stdout
    (root / "probe.py").write_text(PROBE)
    (root / "probe.py").chmod(0o700)
    (root / "go.mod").write_text("module github.com/iryzhkov/t3-steward/qualification\n\ngo 1.25.0\n\nrequire github.com/iryzhkov/t3-steward v0.0.0\nreplace github.com/iryzhkov/t3-steward => " + str(source) + "\n")
    (root / "render.go").write_text(RENDER)
    generated = command(["go", "run", "-mod=mod", ".", str(root / "probe.py"), str(root / "probe.json")], cwd=root).stdout
    assert "NoNewPrivileges=true\n" in generated
    assert "PartOf=" not in generated and "After=" not in generated
    # These are the generator's exact bytes; only InstallOptions select an
    # inert test executable and evidence path instead of any live daemon.
    unit_path = root / unit_name
    unit_path.write_text(generated)
    host_uid = Path("/etc/ssh/ssh_config").stat().st_uid
    linked = False
    try:
        result = command(["systemctl", "--user", "link", "--runtime", str(unit_path)])
        linked = True
        (root / "link.txt").write_text(result.stdout + result.stderr)
        command(["systemctl", "--user", "daemon-reload"])
        result = command(["systemctl", "--user", "start", "--wait", unit_name])
        (root / "start.txt").write_text(result.stdout + result.stderr)
        effective = command(["systemctl", "--user", "show", unit_name,
                             "-p", "ProtectSystem", "-p", "NoNewPrivileges",
                             "-p", "PrivateUsers", "-p", "Result", "-p", "ExecMainStatus"]).stdout
        (root / "effective-unit.txt").write_text(effective)
        probe = json.loads((root / "probe.json").read_text())
        assert probe["sshExit"] == args.expect_ssh, probe
        assert probe["noNewPrivileges"].split()[-1] == "1", probe
        assert "NoNewPrivileges=yes\n" in effective, effective
        assert "ProtectSystem=" + args.expect_protection + "\n" in effective, effective
        if args.expect_ssh:
            assert "Bad owner or permissions" in probe["stderr"], probe
            assert host_uid == 0 and probe["sshConfigUID"] != 0, probe
        else:
            assert host_uid == probe["sshConfigUID"] == 0, probe
            assert "hostname 127.0.0.1\n" in probe["stdout"], probe
        report = {"passed": True, "source": str(source), "sourceCommit": source_commit,
                  "sourceStatus": source_status, "unit": unit_name, "root": str(root),
                  "expectedSSHExit": args.expect_ssh, "hostSSHConfigUID": host_uid,
                  "effectiveUnit": effective, "probe": probe}
        (root / "verdict.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({k: report[k] for k in ["passed", "sourceCommit", "unit", "root", "expectedSSHExit"]}), flush=True)
    finally:
        if linked:
            subprocess.run(["systemctl", "--user", "stop", unit_name], capture_output=True, timeout=20)
            cleanup = command(["systemctl", "--user", "disable", "--runtime", unit_name])
            (root / "cleanup.txt").write_text(cleanup.stdout + cleanup.stderr)
            command(["systemctl", "--user", "daemon-reload"])
            loaded = command(["systemctl", "--user", "show", unit_name, "-p", "LoadState"]).stdout
            assert loaded.strip() == "LoadState=not-found", loaded
            (root / "cleanup.txt").open("a").write(loaded)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
