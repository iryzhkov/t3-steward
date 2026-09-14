#!/usr/bin/env bash
# Disposable qualification fleet for the hardening gate 1 harness.
#
# Everything this file creates lives under one temporary root that the caller
# owns and that run.sh removes again. Nothing is read from or written to the
# real home: every process started here is given its own HOME, XDG_CONFIG_HOME,
# XDG_STATE_HOME and T3 data directory, and the coordinator, the workers and the
# client each get their own configuration, state, sockets, journals, workspaces
# and secret store.
#
# The restricted SSH path is a real OpenSSH sshd, started as an unprivileged
# process on a loopback high port with its own host key and its own
# authorized_keys file. The forced-command lines are the ones the operations
# runbook documents, with one wrapper interposed: the wrapper exists only to
# replace the ambient home of the login account with the disposable one, and it
# neither reads nor forwards SSH_ORIGINAL_COMMAND.

set -euo pipefail

# fleet_fail reports a harness fault. A harness that cannot build its own fleet
# must stop loudly rather than continue and report a case as skipped.
fleet_fail() {
  printf 'harness error: %s\n' "$*" >&2
  exit 1
}

fleet_log() { printf '[harness] %s\n' "$*" >&2; }

# fleet_require checks the tools the harness cannot work without.
fleet_require() {
  local missing=()
  local tool
  for tool in go git ssh ssh-keygen python3 curl git-upload-pack; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  [ -x /usr/bin/sshd ] || [ -x /usr/sbin/sshd ] || missing+=(sshd)
  if [ "${#missing[@]}" -ne 0 ]; then
    fleet_fail "missing required tools: ${missing[*]}"
  fi
  FLEET_SSHD=/usr/bin/sshd
  [ -x "$FLEET_SSHD" ] || FLEET_SSHD=/usr/sbin/sshd
}

# fleet_free_port asks the kernel for an unused loopback port.
fleet_free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    print(s.getsockname()[1])
PY
}

fleet_secret() { head -c 32 /dev/urandom | base64 -w0; }

# QUAL_T3_PROJECTS is every T3 project the configured catalog names. The
# synthetic provider is seeded with them so that a worker observing its own
# readiness finds them.
QUAL_T3_PROJECTS="qual-good,qual-private,qual-missing-ref,qual-absent-forge,qual-private-https,qual-plain,qual-beside,qual-park,qual-park-restart,qual-race,qual-bad-setup,qual-bad-syntax,qual-argument-injection"

# fleet_private_file writes content to a 0600 file, which is what both secret
# stores require.
fleet_private_file() {
  local path=$1
  mkdir -p "$(dirname "$path")"
  cat >"$path"
  chmod 0600 "$path"
}

# fleet_track records a background process so cleanup can stop it even when a
# case fails half way through.
fleet_track() { FLEET_PIDS+=("$1"); }

fleet_stop_all() {
  local pid
  for pid in "${FLEET_PIDS[@]:-}"; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${FLEET_PIDS[@]:-}"; do
    [ -n "$pid" ] || continue
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "$pid" 2>/dev/null || true
  done
  FLEET_PIDS=()
}

# fleet_init creates the temporary root and the directory skeleton.
fleet_init() {
  FLEET_PIDS=()
  ROOT=$(mktemp -d "${TMPDIR:-/tmp}/t3qual.XXXXXX")
  export ROOT
  mkdir -p \
    "$ROOT/bin" "$ROOT/ssh" "$ROOT/keys" "$ROOT/forced" "$ROOT/evidence" \
    "$ROOT/repos" "$ROOT/campaigns" "$ROOT/t3" "$ROOT/turns" "$ROOT/signals" \
    "$ROOT/coordinator/home" "$ROOT/coordinator/drop" \
    "$ROOT/coordinator/bundles" "$ROOT/coordinator/artifacts" \
    "$ROOT/coordinator/workspaces" \
    "$ROOT/worker-a/home" "$ROOT/worker-a/storage" \
    "$ROOT/worker-b/home" "$ROOT/worker-b/storage" \
    "$ROOT/client/home"
  chmod 0700 "$ROOT"
  fleet_log "temporary root $ROOT"
}

# fleet_build compiles the binary under test from this worktree. The harness
# never uses an installed t3-steward: the point is to qualify this source.
fleet_build() {
  local repo=$1
  fleet_log "building t3-steward from $repo"
  ( cd "$repo" && CGO_ENABLED=0 go build -o "$ROOT/bin/t3-steward" ./cmd/t3-steward ) \
    || fleet_fail "go build failed"
  STEWARD="$ROOT/bin/t3-steward"
  export STEWARD
}

# fleet_keys generates every SSH identity the fleet uses. One key per forced
# command, because OpenSSH takes the options of the first authorized_keys line
# carrying the presented key.
fleet_keys() {
  local name
  ssh-keygen -q -t ed25519 -N '' -f "$ROOT/ssh/host_key" -C qual-host
  for name in admin admin-query worker-a worker-b repo-good repo-noref repo-private repo-missing repo-unauthorized; do
    ssh-keygen -q -t ed25519 -N '' -f "$ROOT/keys/$name" -C "qual-$name"
  done
}

# fleet_ssh_wrapper interposes an ssh wrapper on the PATH of every process the
# harness starts.
#
# It exists for one reason, and the reason matters when reading the results:
# OpenSSH resolves ~/.ssh/config from the passwd database rather than from
# $HOME, so a process given a disposable home still reads the real account's
# ssh configuration and cannot be given one of its own. The wrapper adds
# -F <disposable ssh_config> and passes every other argument through unchanged.
# Nothing else about the SSH path is simulated: the server is a real sshd, the
# restriction is a real forced command, and the key selection is real.
fleet_ssh_wrapper() {
  cat >"$ROOT/bin/ssh" <<'EOF'
#!/bin/sh
# Disposable qualification ssh wrapper: supplies the configuration file that
# OpenSSH would otherwise read from the real account's home directory.
exec /usr/bin/ssh -F "$T3_QUAL_SSH_CONFIG" "$@"
EOF
  chmod 0755 "$ROOT/bin/ssh"
}

# fleet_wrapper writes one forced-command wrapper.
#
# The wrapper replaces the login account's ambient environment with the
# disposable one and then executes exactly the command the runbook documents.
# It takes no argument from the SSH invocation, does not read
# SSH_ORIGINAL_COMMAND, and does not pass anything to a shell.
fleet_wrapper() {
  local path=$1 home=$2 sshconfig=$3
  shift 3
  {
    printf '#!/bin/sh\n'
    printf '# Disposable qualification forced command. SSH_ORIGINAL_COMMAND is never read.\n'
    printf 'set -eu\n'
    printf 'unset SSH_ORIGINAL_COMMAND\n'
    printf 'HOME=%s\n' "$home"
    printf 'XDG_CONFIG_HOME=%s/.config\n' "$home"
    printf 'XDG_STATE_HOME=%s/.local/state\n' "$home"
    printf 'XDG_DATA_HOME=%s/.local/share\n' "$home"
    printf 'XDG_CACHE_HOME=%s/.cache\n' "$home"
    printf 'GIT_TERMINAL_PROMPT=0\n'
    printf 'GIT_CONFIG_NOSYSTEM=1\n'
    printf 'T3_QUAL_SSH_CONFIG=%s\n' "$sshconfig"
    printf 'PATH=%s/bin:/usr/bin:/bin\n' "$ROOT"
    # The worker runs its child processes in transient systemd user scopes, so
    # it needs the session bus this sshd does not set up. On a real host
    # pam_systemd provides both; sshd here runs without PAM, so they are passed
    # through explicitly. They name the session, never the real home.
    printf 'XDG_RUNTIME_DIR=%s\n' "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
    printf 'DBUS_SESSION_BUS_ADDRESS=%s\n' "${DBUS_SESSION_BUS_ADDRESS:-unix:path=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/bus}"
    printf 'export HOME XDG_CONFIG_HOME XDG_STATE_HOME XDG_DATA_HOME XDG_CACHE_HOME GIT_TERMINAL_PROMPT GIT_CONFIG_NOSYSTEM T3_QUAL_SSH_CONFIG PATH XDG_RUNTIME_DIR DBUS_SESSION_BUS_ADDRESS\n'
    printf 'exec'
    local argument
    for argument in "$@"; do
      printf " '%s'" "$argument"
    done
    printf '\n'
  } >"$path"
  chmod 0700 "$path"
}

# fleet_authorize appends one restricted authorized_keys line.
fleet_authorize() {
  local key=$1 command=$2
  printf 'restrict,command="%s" %s\n' "$command" "$(cat "$ROOT/keys/$key.pub")" \
    >>"$ROOT/ssh/authorized_keys"
  chmod 0600 "$ROOT/ssh/authorized_keys"
}

# fleet_ssh_config writes one disposable ssh configuration. Every alias pins its
# own identity, its own known-hosts file and the loopback port the disposable
# sshd listens on, which is how a client selects the key a forced command is
# bound to without any value reaching an ssh command line.
fleet_ssh_config() {
  local file=$1
  shift
  mkdir -p "$(dirname "$file")"
  : >"$file"
  local pair alias key
  for pair in "$@"; do
    alias=${pair%%=*}
    key=${pair#*=}
    cat >>"$file" <<EOF
Host $alias
  HostName 127.0.0.1
  Port $SSH_PORT
  User $(id -un)
  IdentityFile $ROOT/keys/$key
  IdentitiesOnly yes
  BatchMode yes
  StrictHostKeyChecking yes
  UserKnownHostsFile $ROOT/ssh/known_hosts
  PasswordAuthentication no

EOF
  done
  chmod 0600 "$file"
}

# fleet_sshd starts the disposable OpenSSH server. It is a real sshd: the
# forced-command restriction under test is enforced by sshd itself, not by a
# stand-in.
fleet_sshd() {
  SSH_PORT=$(fleet_free_port)
  export SSH_PORT
  cat >"$ROOT/ssh/sshd_config" <<EOF
Port $SSH_PORT
ListenAddress 127.0.0.1
HostKey $ROOT/ssh/host_key
PidFile $ROOT/ssh/sshd.pid
AuthorizedKeysFile $ROOT/ssh/authorized_keys
StrictModes no
UsePAM no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
PermitRootLogin no
AllowTcpForwarding no
X11Forwarding no
PermitTTY no
LogLevel VERBOSE
EOF
  "$FLEET_SSHD" -f "$ROOT/ssh/sshd_config" -E "$ROOT/evidence/sshd.log" \
    || fleet_fail "could not start the disposable sshd"
  local deadline=$((SECONDS + 10))
  while [ ! -s "$ROOT/ssh/sshd.pid" ]; do
    [ "$SECONDS" -lt "$deadline" ] || fleet_fail "the disposable sshd did not write a pid file"
    sleep 0.1
  done
  fleet_track "$(cat "$ROOT/ssh/sshd.pid")"
  printf '[127.0.0.1]:%s %s\n' "$SSH_PORT" "$(cat "$ROOT/ssh/host_key.pub")" \
    >"$ROOT/ssh/known_hosts"
  fleet_log "disposable sshd on 127.0.0.1:$SSH_PORT (pid $(cat "$ROOT/ssh/sshd.pid"))"
}

# fleet_repositories creates the throwaway Git repositories. They are served
# over the same disposable sshd through forced git-upload-pack commands, so the
# repository probe exercises a real ssh:// remote rather than a local path.
fleet_repositories() {
  local name
  for name in good private noref; do
    git init -q --bare "$ROOT/repos/$name.git"
  done
  local work="$ROOT/repos/work"
  git init -q "$work"
  git -C "$work" config user.email qualification@invalid
  git -C "$work" config user.name Qualification
  printf 'disposable qualification fixture\n' >"$work/README.md"
  git -C "$work" add README.md
  git -C "$work" commit -q -m 'Add the disposable qualification fixture.'
  git -C "$work" branch -M main
  git -C "$work" push -q "$ROOT/repos/good.git" main
  git -C "$work" push -q "$ROOT/repos/private.git" main
  # noref.git deliberately keeps a branch nobody asks for, so a probe for main
  # reaches a repository that answers and has no such ref.
  git -C "$work" push -q "$ROOT/repos/noref.git" main:refs/heads/other
}

# fleet_forced_commands installs every wrapper and authorized_keys line.
fleet_forced_commands() {
  : >"$ROOT/ssh/authorized_keys"

  # The coordinator admin transport. One line per operation, exactly as the
  # operations runbook documents.
  # Both documented shapes of the admin forced command are installed, because
  # both are supported and a harness that exercised only one could not catch a
  # regression in the other.
  #
  # The unpinned line carries no operation word: the operation is taken from the
  # signed frame, so one key serves a client that needs several operations. It
  # is what an agent's ordinary "campaign submit" travels over, since submit
  # issues a viability query and then a submission.
  fleet_wrapper "$ROOT/forced/admin" "$ROOT/coordinator/home" "$ROOT/coordinator/ssh_config" \
    "$STEWARD" coordinator-exchange --config "$ROOT/coordinator/config.yaml"
  # The pinned line names one operation, which narrows the key to it.
  fleet_wrapper "$ROOT/forced/admin-query" "$ROOT/coordinator/home" "$ROOT/coordinator/ssh_config" \
    "$STEWARD" coordinator-exchange --config "$ROOT/coordinator/config.yaml" query
  fleet_authorize admin "$ROOT/forced/admin"
  fleet_authorize admin-query "$ROOT/forced/admin-query"

  # The two workers. Each is reached through one persistent bridge rather than
  # through the three fixed worker-exchange operations, because a coordinator
  # dials one address per worker while a forced command pins one operation word:
  # a worker whose key is pinned to "control" can never take artifact delivery.
  # The persistent connection multiplexes control, artifact-send and
  # artifact-receive over one stream, so one key serves the worker.
  local worker
  for worker in worker-a worker-b; do
    fleet_wrapper "$ROOT/forced/$worker-bridge" "$ROOT/$worker/home" "$ROOT/$worker/ssh_config" \
      "$STEWARD" worker --config "$ROOT/$worker/config.yaml" bridge
    fleet_authorize "$worker" "$ROOT/forced/$worker-bridge"
  done

  # The repositories. Each key serves exactly one repository through
  # git-upload-pack, so a key that is not authorized cannot read any of them.
  fleet_wrapper "$ROOT/forced/repo-good" "$ROOT/repos" "$ROOT/repos/ssh_config" \
    git-upload-pack "$ROOT/repos/good.git"
  fleet_wrapper "$ROOT/forced/repo-private" "$ROOT/repos" "$ROOT/repos/ssh_config" \
    git-upload-pack "$ROOT/repos/private.git"
  fleet_wrapper "$ROOT/forced/repo-noref" "$ROOT/repos" "$ROOT/repos/ssh_config" \
    git-upload-pack "$ROOT/repos/noref.git"
  fleet_authorize repo-good "$ROOT/forced/repo-good"
  fleet_authorize repo-private "$ROOT/forced/repo-private"
  fleet_authorize repo-noref "$ROOT/forced/repo-noref"

  # A forge that does not have the repository. Forgejo answers an unknown
  # repository over SSH with this wording, which is the message the campaign
  # that motivated the readiness check actually received. The key is authorized;
  # the repository is the thing that is missing.
  fleet_wrapper "$ROOT/forced/repo-missing" "$ROOT/repos" "$ROOT/repos/ssh_config" \
    "$ROOT/forced/forgejo-missing.sh"
  cat >"$ROOT/forced/forgejo-missing.sh" <<'EOF'
#!/bin/sh
echo "Forgejo: Cannot find repository: qualification/absent" >&2
exit 1
EOF
  chmod 0700 "$ROOT/forced/forgejo-missing.sh"
  fleet_authorize repo-missing "$ROOT/forced/repo-missing"
}

# fleet_secrets writes the disposable credential stores. Admin and worker
# references live in disjoint namespaces, exactly as they do in production, and
# the same bytes are written to both ends of each pair because both ends resolve
# the reference independently.
fleet_secrets() {
  local client_secret coordinator_secret
  client_secret=$(fleet_secret)
  coordinator_secret=$(fleet_secret)
  ADMIN_PRINCIPAL="admin:qual-client"
  export ADMIN_PRINCIPAL
  local admin_json
  admin_json=$(printf '{"clientPrincipal":"%s","clientKeyId":"qual-client-key","clientSecret":"%s","coordinatorPrincipal":"qual-coordinator","coordinatorKeyId":"qual-coordinator-key","coordinatorSecret":"%s"}' \
    "$ADMIN_PRINCIPAL" "$client_secret" "$coordinator_secret")
  local home
  for home in "$ROOT/client/home" "$ROOT/coordinator/home"; do
    printf '%s' "$admin_json" | fleet_private_file "$home/.config/upkeeper/secrets/f03-admin/qual-client"
  done

  local worker
  for worker in worker-a worker-b; do
    local worker_secret
    worker_secret=$(fleet_secret)
    local protocol_json
    protocol_json=$(printf '{"coordinatorPrincipal":"ssh:qual-coordinator","coordinatorKeyId":"qual-coordinator-key","coordinatorSecret":"%s","workerPrincipal":"ssh:%s","workerKeyId":"qual-worker-key","workerSecret":"%s"}' \
      "$coordinator_secret" "$worker" "$worker_secret")
    for home in "$ROOT/coordinator/home" "$ROOT/$worker/home"; do
      printf '%s' "$protocol_json" \
        | fleet_private_file "$home/.config/upkeeper/secrets/f02-protocol/$worker"
    done
  done
}

# fleet_t3_stub starts the synthetic provider. No real agent turn is executed by
# this harness: the fleet's Claude and Codex quota is live and shared, and the
# contract calls for synthetic provider observations. The stub records every
# request it receives, so "no turn was dispatched" is evidence rather than an
# assumption.
fleet_t3_stub() {
  T3_PORT=$(fleet_free_port)
  export T3_PORT
  python3 "$HARNESS_DIR/t3_stub.py" "$T3_PORT" "$ROOT/evidence/t3-stub.jsonl" "$ROOT/turns" \
    "$QUAL_T3_PROJECTS" >"$ROOT/evidence/t3-stub.log" 2>&1 &
  fleet_track "$!"
  local deadline=$((SECONDS + 10))
  until curl -sf "http://127.0.0.1:$T3_PORT/health" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || fleet_fail "the synthetic provider did not start"
    sleep 0.2
  done
  fleet_log "synthetic provider on 127.0.0.1:$T3_PORT"
}

# fleet_worker_config writes one worker configuration.
fleet_worker_config() {
  local worker=$1 repo_key=$2
  cat >"$ROOT/$worker/config.yaml" <<EOF
state_path: $ROOT/$worker/state.db
log_level: info
quota_checks: false
t3:
  url: http://127.0.0.1:$T3_PORT
  data_dir: $ROOT/$worker/t3
  token: qualification-synthetic-token
  allow_unsupported_version: true
policy:
  # The worker must take the real execution path. In dry-run it reads a file
  # instead of the provider, and Collect returns before anything is captured,
  # so no lifecycle case could observe an output or a verification.
  dry_run: false
backlog_v2:
  mode: worker
  coordinator:
    id: qual-coordinator
  local_worker:
    id: $worker
    epoch: $worker-epoch-1
    coordinator_epoch: 1
  workers:
$(fleet_worker_block)
  projects:
$(fleet_project_block)
  setup_profiles:
    quick:
      commands: ["true"]
      timeout: 1m
    broken:
      commands: ["false"]
      timeout: 1m
  quota_pools:
    synthetic-pool:
      provider: synthetic
      max_concurrent: 2
  storage:
    bundles: $ROOT/$worker/storage/bundles
    artifacts: $ROOT/$worker/storage/artifacts
    workspaces: $ROOT/$worker/storage/workspaces
  transport:
    kind: ssh
    request_timeout: 30s
  message_limits:
    max_bytes: 4194304
    max_files: 1000
    max_artifact_bytes: 16777216
  freshness:
    worker_max_age: 5m
    quota_max_age: 8760h
  scheduling:
    interval: 2s
  startup_admission: closed
EOF
  fleet_ssh_config "$ROOT/$worker/ssh_config" \
    "qual-repo-good=repo-good" \
    "qual-repo-noref=repo-noref" \
    "qual-forge=repo-missing" \
    "qual-repo-private=$repo_key"
}

# fleet_provider_cache writes the disposable T3 provider cache each worker reads
# to observe which provider instances and models it can actually serve. Without
# it the worker reports the configured route unavailable and enrollment is
# refused, which is the correct behaviour and not what these cases are about.
fleet_provider_cache() {
  local worker=$1
  mkdir -p "$ROOT/$worker/t3/caches"
  cat >"$ROOT/$worker/t3/caches/synthetic.json" <<'EOF'
{
  "instanceId": "synthetic",
  "enabled": true,
  "installed": true,
  "status": "ready",
  "auth": {"status": "authenticated"},
  "models": [{"slug": "synthetic-model"}]
}
EOF
}

# fleet_worker_bootstrap writes the UpKeeper-owned worker bootstrap document the
# persistent worker reads at startup. It is a closed field set with a strict
# decoder, so the capability and route lists must be sorted and the credential
# reference must name this worker.
fleet_worker_bootstrap() {
  local worker=$1
  printf '%s' "{\"schema_version\":1,\"worker_id\":\"$worker\",\"coordinator_id\":\"qual-coordinator\",\"transport\":\"ssh\",\"capabilities\":[\"git\",\"huyang\"],\"provider_routes\":[\"synthetic\"],\"credential_ref\":\"secretref:f02-protocol/$worker\"}" \
    | fleet_private_file "$ROOT/$worker/home/.config/t3-steward/worker-bootstrap.json"
}

# fleet_start_workers starts one persistent worker daemon per worker. The daemon
# owns the worker's Unix socket; the SSH bridge forced command is what the
# coordinator reaches it through.
fleet_start_workers() {
  local worker
  for worker in worker-a worker-b; do
    (
      cd "$ROOT/$worker"
      HOME="$ROOT/$worker/home" \
      XDG_CONFIG_HOME="$ROOT/$worker/home/.config" \
      XDG_STATE_HOME="$ROOT/$worker/home/.local/state" \
      XDG_DATA_HOME="$ROOT/$worker/home/.local/share" \
      XDG_CACHE_HOME="$ROOT/$worker/home/.cache" \
      XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}" \
      GIT_TERMINAL_PROMPT=0 GIT_CONFIG_NOSYSTEM=1 \
      T3_QUAL_SSH_CONFIG="$ROOT/$worker/ssh_config" \
      PATH="$ROOT/bin:/usr/bin:/bin" \
      exec "$STEWARD" worker --config "$ROOT/$worker/config.yaml" serve
    ) >>"$ROOT/evidence/$worker.log" 2>&1 &
    eval "${worker//-/_}_PID=\$!"
    fleet_track "$!"
    local socket="$ROOT/$worker/home/.local/state/t3-steward/worker/worker.sock"
    local deadline=$((SECONDS + 30))
    until [ -S "$socket" ]; do
      [ "$SECONDS" -lt "$deadline" ] \
        || fleet_fail "$worker did not open its socket: $(tail -n 10 "$ROOT/evidence/$worker.log")"
      sleep 0.2
    done
    fleet_log "$worker serving on $socket"
  done
}

# fleet_restart_worker stops one persistent worker daemon and starts it again.
# The socket lock is released on exit, so the replacement can take it.
fleet_restart_worker() {
  local worker=$1
  local variable="${worker//-/_}_PID"
  local pid=${!variable:-}
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    local deadline=$((SECONDS + 30))
    while kill -0 "$pid" 2>/dev/null; do
      [ "$SECONDS" -lt "$deadline" ] || { kill -9 "$pid" 2>/dev/null || true; break; }
      sleep 0.2
    done
  fi
  local socket="$ROOT/$worker/home/.local/state/t3-steward/worker/worker.sock"
  rm -f "$socket"
  (
    cd "$ROOT/$worker"
    HOME="$ROOT/$worker/home" \
    XDG_CONFIG_HOME="$ROOT/$worker/home/.config" \
    XDG_STATE_HOME="$ROOT/$worker/home/.local/state" \
    XDG_DATA_HOME="$ROOT/$worker/home/.local/share" \
    XDG_CACHE_HOME="$ROOT/$worker/home/.cache" \
    XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}" \
    GIT_TERMINAL_PROMPT=0 GIT_CONFIG_NOSYSTEM=1 \
    T3_QUAL_SSH_CONFIG="$ROOT/$worker/ssh_config" \
    PATH="$ROOT/bin:/usr/bin:/bin" \
    exec "$STEWARD" worker --config "$ROOT/$worker/config.yaml" serve
  ) >>"$ROOT/evidence/$worker.log" 2>&1 &
  eval "$variable=\$!"
  fleet_track "$!"
  local deadline=$((SECONDS + 30))
  until [ -S "$socket" ]; do
    [ "$SECONDS" -lt "$deadline" ] || fleet_fail "$worker did not reopen its socket"
    sleep 0.2
  done
  fleet_log "$worker restarted"
}

# fleet_enroll_workers admits each worker to the coordinator's current catalog.
# Enrollment is an operator action on the coordinator host, and the persistent
# path requires it before any assignment can be offered.
fleet_enroll_workers() {
  local worker digest deadline
  for worker in worker-a worker-b; do
    deadline=$((SECONDS + 60))
    while :; do
      digest=$(fleet_coordinator_cli backlog workers --json 2>/dev/null \
        | python3 "$HARNESS_DIR/inspect.py" worker-catalog "$worker")
      [ -n "$digest" ] && [ "$digest" != unreadable ] && break
      [ "$SECONDS" -lt "$deadline" ] || fleet_fail "no catalog revision for $worker"
      sleep 1
    done
    if ! fleet_coordinator_cli worker enroll "$worker" \
        --request-id "qual-enroll-$worker" --catalog-revision "$digest" \
        --reason 'disposable qualification fleet' --expected-revision 0 \
        >"$ROOT/evidence/enroll-$worker.json" 2>&1; then
      fleet_fail "enrolling $worker failed: $(tail -n 5 "$ROOT/evidence/enroll-$worker.json")"
    fi
    fleet_log "$worker enrolled against catalog $digest"
  done
}

# fleet_worker_block is the shared worker catalog. Both workers and the
# coordinator must agree on it.
fleet_worker_block() {
  local worker connection="      connection: persistent-ssh"
  # The malformed catalog is served by a coordinator with no persistent worker.
  # A project whose repository syntax is invalid makes the worker execution
  # catalog fail to build, and with a persistent worker that happens while the
  # coordinator is starting, so the coordinator exits instead of reporting the
  # project as misconfigured.
  if [ "${1:-clean}" = malformed ]; then
    connection="      # no persistent connection in the malformed variant"
  fi
  for worker in worker-a worker-b; do
    cat <<EOF
    $worker:
      address: qual-$worker
$connection
      epoch: $worker-epoch-1
      accept_backlog: true
      capabilities: [git, huyang]
      credential: secretref:f02-protocol/$worker
      providers:
        synthetic:
          models: [synthetic-model]
          quota_pool: synthetic-pool
EOF
  done
}

# fleet_project_block is the shared project catalog. Each project exists to make
# one repository observation reachable from a campaign.
#
# The variant argument selects whether the two malformed projects are included.
# They cannot be present while the fleet needs working workers: a project whose
# repository syntax is invalid makes the whole worker execution-package catalog
# fail to build, so every worker reconciliation fails and the coordinator holds
# no worker snapshot at all. The harness therefore proves the syntax refusals
# against a coordinator restarted with them, and everything else against a
# coordinator whose catalog is clean.
fleet_project_block() {
  cat <<EOF
    good:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-good
      setup_profile: quick
      workers: [worker-a, worker-b]
    private:
      repository: ssh://qual-repo-private/private.git
      default_ref: main
      t3_project: qual-private
      setup_profile: quick
      workers: [worker-a, worker-b]
    missing-ref:
      repository: ssh://qual-repo-noref/noref.git
      default_ref: main
      t3_project: qual-missing-ref
      setup_profile: quick
      workers: [worker-a, worker-b]
    absent-forge:
      repository: ssh://qual-forge/qualification/absent.git
      default_ref: main
      t3_project: qual-absent-forge
      setup_profile: quick
      workers: [worker-a, worker-b]
    private-https:
      repository: $QUAL_PRIVATE_HTTPS
      default_ref: main
      t3_project: qual-private-https
      setup_profile: quick
      workers: [worker-a, worker-b]
    plain:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-plain
      setup_profile: quick
      workers: [worker-a]
    beside:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-beside
      setup_profile: quick
      workers: [worker-b]
    park:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-park
      setup_profile: quick
      workers: [worker-a]
    park-restart:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-park-restart
      setup_profile: quick
      workers: [worker-a]
    race:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-race
      setup_profile: quick
      workers: [worker-a]
    bad-setup:
      repository: ssh://qual-repo-good/good.git
      default_ref: main
      t3_project: qual-bad-setup
      setup_profile: broken
      workers: [worker-a]
EOF
  [ "${1:-clean}" = malformed ] || return 0
  cat <<EOF
    bad-syntax:
      repository: "file:///not/an/allowed/scheme"
      default_ref: main
      t3_project: qual-bad-syntax
      setup_profile: quick
      workers: [worker-a, worker-b]
    argument-injection:
      repository: "--upload-pack=touch /tmp/qual-injection"
      default_ref: main
      t3_project: qual-argument-injection
      setup_profile: quick
      workers: [worker-a, worker-b]
EOF
}

# fleet_coordinator_config writes the coordinator configuration and its ssh
# aliases for the two workers. The variant selects the project catalog.
fleet_coordinator_config() {
  local variant=${1:-clean}
  cat >"$ROOT/coordinator/config.yaml" <<EOF
state_path: $ROOT/coordinator/state.db
log_level: info
quota_checks: false
t3:
  url: http://127.0.0.1:$T3_PORT
  data_dir: $ROOT/t3
  token: qualification-synthetic-token
  allow_unsupported_version: true
policy:
  # The coordinator's own wait runner refuses to resume a parked attempt in a
  # dry run, and its T3 adapter would log the wake instead of sending it.
  dry_run: false
wait:
  dry_run: false
backlog:
  dir: $ROOT/coordinator/drop
backlog_v2:
  mode: coordinator
  coordinator:
    id: qual-coordinator
    admin_clients:
      $ADMIN_PRINCIPAL:
        credential: secretref:f03-admin/qual-client
  workers:
$(fleet_worker_block "$variant")
  projects:
$(fleet_project_block "$variant")
  setup_profiles:
    quick:
      commands: ["true"]
      timeout: 1m
    broken:
      commands: ["false"]
      timeout: 1m
  quota_pools:
    synthetic-pool:
      # One at a time on purpose: it is what makes "a parked attempt released
      # its slot" observable, because a second campaign can only run while the
      # first is parked if the slot really came back.
      provider: synthetic
      max_concurrent: 1
  storage:
    bundles: $ROOT/coordinator/bundles
    artifacts: $ROOT/coordinator/artifacts
    workspaces: $ROOT/coordinator/workspaces
  transport:
    kind: ssh
    request_timeout: 30s
  message_limits:
    max_bytes: 4194304
    max_files: 1000
    # A persistent connection caps the artifact limit at 16 MiB.
    max_artifact_bytes: 16777216
  freshness:
    # A persistent connection caps worker freshness at five minutes.
    worker_max_age: 5m
    # Quota observations never arrive: the fleet's provider is synthetic and
    # quota checks are off. A short bound would add a temporary reason to every
    # candidate and obscure the finding each case is about.
    quota_max_age: 8760h
  scheduling:
    interval: 2s
  startup_admission: closed
EOF
  fleet_ssh_config "$ROOT/coordinator/ssh_config" \
    "qual-worker-a=worker-a" \
    "qual-worker-b=worker-b"
}

# fleet_client_config writes the configuration of the host that is not the
# coordinator.
#
# Two profiles are written, one per documented forced-command shape. "main"
# names the unpinned line and is the ordinary agent path: one client block, one
# key, every operation. "pinned" names the line that pins the query operation,
# so the narrower arrangement stays exercised as well.
fleet_client_config() {
  local name key
  for name in main pinned; do
    key=admin
    if [ "$name" = pinned ]; then
      key=admin-query
    fi
    cat >"$ROOT/client/config-$name.yaml" <<EOF
state_path: $ROOT/client/state.db
log_level: info
t3:
  url: http://127.0.0.1:$T3_PORT
  data_dir: $ROOT/client/t3
  token: qualification-synthetic-token
  allow_unsupported_version: true
policy:
  dry_run: true
backlog_v2:
  mode: disabled
  coordinator_client:
    coordinator_id: qual-coordinator
    address: qual-$key
    connection: ssh
    remote_command: $STEWARD
    credential: secretref:f03-admin/qual-client
    request_timeout: 30s
    message_limits:
      max_bytes: 4194304
      max_files: 1000
      max_artifact_bytes: 1073741824
EOF
  done
  fleet_ssh_config "$ROOT/client/ssh_config" \
    "qual-admin=admin" \
    "qual-admin-query=admin-query"
}

# fleet_start_coordinator starts the coordinator process and waits for its
# owner-only admin socket.
fleet_start_coordinator() {
  (
    cd "$ROOT/coordinator"
    HOME="$ROOT/coordinator/home" \
    XDG_CONFIG_HOME="$ROOT/coordinator/home/.config" \
    XDG_STATE_HOME="$ROOT/coordinator/home/.local/state" \
    XDG_DATA_HOME="$ROOT/coordinator/home/.local/share" \
    XDG_CACHE_HOME="$ROOT/coordinator/home/.cache" \
    GIT_TERMINAL_PROMPT=0 GIT_CONFIG_NOSYSTEM=1 \
    T3_QUAL_SSH_CONFIG="$ROOT/coordinator/ssh_config" \
    PATH="$ROOT/bin:/usr/bin:/bin" \
    XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}" \
    exec "$STEWARD" run --config "$ROOT/coordinator/config.yaml"
  ) >"$ROOT/evidence/coordinator.log" 2>&1 &
  COORDINATOR_PID=$!
  export COORDINATOR_PID
  fleet_track "$COORDINATOR_PID"
  local socket="$ROOT/coordinator/state.db.admin.sock"
  local deadline=$((SECONDS + 30))
  until [ -S "$socket" ]; do
    kill -0 "$COORDINATOR_PID" 2>/dev/null \
      || fleet_fail "the coordinator exited before it served its admin socket: $(tail -n 20 "$ROOT/evidence/coordinator.log")"
    [ "$SECONDS" -lt "$deadline" ] || fleet_fail "the coordinator admin socket never appeared"
    sleep 0.2
  done
  fleet_log "coordinator pid $COORDINATOR_PID, admin socket $socket"
}

# fleet_restart_coordinator rewrites the coordinator catalog and restarts the
# process. It is how the harness reaches the two malformed projects, which
# cannot coexist with working workers.
fleet_restart_coordinator() {
  local variant=$1
  if [ -n "${COORDINATOR_PID:-}" ]; then
    kill "$COORDINATOR_PID" 2>/dev/null || true
    wait "$COORDINATOR_PID" 2>/dev/null || true
  fi
  fleet_coordinator_config "$variant"
  # Each coordinator lifetime keeps its own log, so a case can count how many
  # times something was reported across restarts.
  FLEET_COORDINATOR_LIVES=$((${FLEET_COORDINATOR_LIVES:-1} + 1))
  mv "$ROOT/evidence/coordinator.log" \
    "$ROOT/evidence/coordinator-life$FLEET_COORDINATOR_LIVES.log" 2>/dev/null || true
  fleet_start_coordinator
  fleet_log "coordinator restarted with the $variant project catalog"
}

# fleet_coordinator_cli runs the CLI on the coordinator host itself, over the
# owner-only socket. It is the harness's own instrument, not a case.
fleet_coordinator_cli() {
  env -i \
    PATH="$ROOT/bin:/usr/bin:/bin" \
    HOME="$ROOT/coordinator/home" \
    XDG_CONFIG_HOME="$ROOT/coordinator/home/.config" \
    XDG_STATE_HOME="$ROOT/coordinator/home/.local/state" \
    T3_QUAL_SSH_CONFIG="$ROOT/coordinator/ssh_config" \
    "$STEWARD" "$1" --config "$ROOT/coordinator/config.yaml" "${@:2}"
}

# fleet_client_cli runs the CLI as the non-coordinator host would: its own home,
# its own secret store, its own coordinator client, and no access to the
# coordinator's socket. The first argument selects the client profile, "main"
# or "pinned".
fleet_client_cli() {
  local profile=$1
  shift
  env -i \
    PATH="$ROOT/bin:/usr/bin:/bin" \
    HOME="$ROOT/client/home" \
    XDG_CONFIG_HOME="$ROOT/client/home/.config" \
    XDG_STATE_HOME="$ROOT/client/home/.local/state" \
    T3_QUAL_SSH_CONFIG="$ROOT/client/ssh_config" \
    "$STEWARD" "$1" --config "$ROOT/client/config-$profile.yaml" "${@:2}"
}

# fleet_await_workers waits until both workers have reported an inventory the
# coordinator considers usable. Without a snapshot the viability matrix reports
# nothing about a worker, so a case that ran before this would prove nothing.
fleet_await_workers() {
  local deadline=$((SECONDS + 90))
  local observed
  while :; do
    observed=$(fleet_coordinator_cli backlog workers --json 2>/dev/null \
      | python3 -c 'import json,sys
try:
    payload = json.load(sys.stdin)
except Exception:
    print(0); raise SystemExit
workers = payload.get("workers") or []
print(sum(1 for w in workers
         if (w.get("snapshot") or {}).get("connected") and not w.get("stale") and w.get("enrolled")))' 2>/dev/null || echo 0)
    [ "$observed" = "2" ] && break
    if [ "$SECONDS" -ge "$deadline" ]; then
      fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/workers-at-timeout.json" 2>&1 || true
      fleet_fail "only $observed of 2 workers reported a usable inventory; see $ROOT/evidence/workers-at-timeout.json and $ROOT/evidence/coordinator.log"
    fi
    sleep 1
  done
  fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/workers.json"
  fleet_log "both workers observed"
}

# fleet_campaign writes one disposable campaign directory naming one project.
fleet_campaign() {
  local name=$1 project=$2
  local dir="$ROOT/campaigns/$name"
  mkdir -p "$dir/prompts"
  cat >"$dir/prompts/task.md" <<'EOF'
This prompt is never executed. The qualification harness submits it to observe
what the coordinator does with the submission, and no provider turn is started.
EOF
  cat >"$dir/workflow.yaml" <<EOF
version: 2
name: $name
class: surplus

environment:
  project: $project
  type: git
  scope: task

routes:
  - instance: synthetic
    model: synthetic-model
    quota_pool: synthetic-pool

tasks:
  only:
    prompt_file: prompts/task.md
    max_turns: 1
EOF
  printf '%s' "$dir"
}

# fleet_executable_campaign writes a campaign whose task is meant to run: it
# declares one output and one verification command, so a case can tell apart
# "nothing was collected" from "the output was collected and verified".
fleet_executable_campaign() {
  local name=$1 project=$2
  local dir="$ROOT/campaigns/$name"
  mkdir -p "$dir/prompts"
  cat >"$dir/prompts/task.md" <<'EOF'
The synthetic provider runs a scripted turn for this task. The script is the
agent: it is what registers a task-bound wait or writes the declared outputs.
EOF
  cat >"$dir/workflow.yaml" <<EOF
version: 2
name: $name
class: required

environment:
  project: $project
  type: git
  scope: task

routes:
  - instance: synthetic
    model: synthetic-model
    quota_pool: synthetic-pool

tasks:
  work:
    prompt_file: prompts/task.md
    outputs:
      - result.txt
    verify:
      - test -s result.txt
    max_turns: 4
EOF
  printf '%s' "$dir"
}

# fleet_turn_script installs one scripted turn for one T3 project.
#
# The script runs with the prepared workspace as its working directory, under
# the synthetic provider, in place of an agent. Everything it invokes is real:
# the steward binary, the coordinator socket and the workspace on disk.
fleet_turn_script() {
  local project=$1 turn=$2
  mkdir -p "$ROOT/turns/$project"
  cat >"$ROOT/turns/$project/turn$turn.sh"
  chmod 0755 "$ROOT/turns/$project/turn$turn.sh"
}

# fleet_task_env is the environment prelude every turn script shares. It gives
# the scripted agent the same disposable home the worker has and puts the
# steward binary on its path.
fleet_task_env() {
  cat <<EOF
export HOME=$ROOT/worker-a/home
export XDG_CONFIG_HOME=$ROOT/worker-a/home/.config
export XDG_STATE_HOME=$ROOT/worker-a/home/.local/state
export XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
export PATH=$ROOT/bin:/usr/bin:/bin
export T3_QUAL_SSH_CONFIG=$ROOT/worker-a/ssh_config
export GIT_TERMINAL_PROMPT=0
STEWARD=$ROOT/bin/t3-steward
COORDINATOR_CONFIG=$ROOT/coordinator/config.yaml
SIGNALS=$ROOT/signals
EVIDENCE=$ROOT/evidence
EOF
}

# fleet_workflow_count reports how many workflow runs the coordinator holds.
fleet_workflow_count() {
  fleet_coordinator_cli backlog list --json 2>/dev/null \
    | python3 -c 'import json,sys
try:
    payload = json.load(sys.stdin)
except Exception:
    print(-1); raise SystemExit
if payload.get("kind") == "error" or payload.get("version") != "backlog.admin/v1":
    print(-1); raise SystemExit
# The workflows key is omitted when there are none, which is zero runs rather
# than an unreadable answer.
print(len(payload.get("workflows") or []))'
}

# fleet_setup builds the whole disposable fleet.
fleet_setup() {
  local repo=$1
  fleet_require
  fleet_init
  fleet_build "$repo"
  fleet_ssh_wrapper
  fleet_keys
  fleet_repositories
  fleet_t3_stub
  fleet_sshd
  fleet_secrets
  fleet_coordinator_config
  fleet_worker_config worker-a repo-private
  # worker-b holds no credential for the private repository. Its alias names a
  # key that no authorized_keys line carries, which is what an absent credential
  # reference looks like to ssh.
  fleet_worker_config worker-b repo-unauthorized
  fleet_client_config
  fleet_forced_commands
  fleet_worker_bootstrap worker-a
  fleet_worker_bootstrap worker-b
  fleet_provider_cache worker-a
  fleet_provider_cache worker-b
  fleet_start_workers
  fleet_start_coordinator
  fleet_enroll_workers
  fleet_await_workers
}
