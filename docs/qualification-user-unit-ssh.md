# Generated user-unit SSH ownership qualification

## Method

`scripts/qualify-user-unit-ssh.py` imports the selected worktree's actual `platform.RenderUnit` through a small disposable Go module. It supplies an inert probe executable and evidence path as `InstallOptions.Binary` and `ConfigPath`, leaving the generated unit bytes intact. No T3 dependency is selected. A unique `t3-qualification-ssh-<suffix>.service` is linked into systemd's runtime user-unit directory, started with `--wait`, then stopped/unlinked. Cleanup requires `LoadState=not-found`.

The probe runs `/usr/bin/ssh -G -F /etc/ssh/ssh_config -o CanonicalizeHostname=no -o ProxyCommand=none -o HostName=127.0.0.1 t3-unit-qualification.invalid`. This reads system SSH configuration and does not connect remotely or read personal SSH configuration. It records SSH exit/output, `/proc/self/uid_map`, `/proc/self/status` NoNewPrivs, and the apparent owner of `/etc/ssh/ssh_config`. The generated Restart policy remains untouched; the probe records SSH's exit instead of propagating the expected negative exit into a restart loop.

No live T3 service, configuration or database was modified. The host ran systemd 261.2-1-arch. This is actual generated-unit execution on this machine, not a mocked systemctl or a unit-string-only assertion.

## Red

- Product baseline `84a0ac3d1ccb1663da2ba3e03dacdf58c0bc5c66`; only the new qualification script was untracked. Generator/source production files were unchanged.
- Command: `python3 -B scripts/qualify-user-unit-ssh.py --source /home/igor/Work/wt-q-unit-ssh-qualification --expect-ssh 255 --expect-protection full`
- Root `/tmp/t3-unit-ssh.zg6cl9ee`; log `/tmp/unit-ssh-red2.log`.
- Effective `ProtectSystem=full`, `NoNewPrivileges=yes`, `PrivateUsers=no`.
- Actual UID map: `1000 1000 1`; SSH configuration appeared owned by UID 65534 instead of host UID 0.
- SSH exit 255: `Bad owner or permissions on /etc/ssh/ssh_config.d/20-omarchy-keepalive.conf`.
- The verifier exited 0 because this exact expected regression was observed.

## Green

- Clean source `1105aa12231ea84893fd556ddbeeeb80cfafa314`.
- Command: `python3 -B scripts/qualify-user-unit-ssh.py --source /home/igor/Work/wt-q-user-unit-ssh --expect-ssh 0 --expect-protection no`
- Root `/tmp/t3-unit-ssh.lg1gs55d`; log `/tmp/unit-ssh-green.log`.
- Effective `ProtectSystem=no`, `NoNewPrivileges=yes`, `PrivateUsers=no`.
- Actual UID map: `0 0 4294967295`; SSH configuration correctly appeared owned by UID 0.
- SSH exited 0 and produced the requested effective hostname. The verifier exited 0.

Both processes reported `NoNewPrivs: 1`. Both evidence roots contain `verdict.json`, exact generated unit, `probe.json`, `effective-unit.txt` and `cleanup.txt`. Both runtime unit links were removed and `LoadState=not-found` was verified. The failed initial script invocation in `/tmp/unit-ssh-red.log` was a Python module-shadowing fixture issue before any systemd action; it is not product evidence.

Harness commit: `4df75b7`. This qualifies the generator's hardening correction. UpKeeper's migration of an existing installed unit is separately owned and requires its own validation.
