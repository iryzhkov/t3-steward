# Security policy

## Scope

The watchdog runs as an unprivileged per-user process. It reads T3's
provider event logs, calls the local T3 HTTP API with a bearer token, and
writes a SQLite database under the user's state directory. It opens no
listening sockets and sends nothing off the machine.

Things worth reporting:

- A way for a log line or an API response to make the watchdog execute
  something, crash persistently, or act on a thread it should not touch.
- Token material or message bodies ending up in logs or the database.
- A generated systemd unit that weakens the user's security posture.

## Reporting

Please do not open a public issue for a vulnerability. Use GitHub's private
vulnerability reporting on this repository ("Report a vulnerability" under
the Security tab). Expect an acknowledgement within a week.

## Supported versions

Only the latest release receives fixes.
