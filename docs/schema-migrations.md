# Schema migrations

Register each coordinator migration in `versionedMigrations` beside its upgrade SQL. Every migration to N > 42 must declare `downgradeDDL` restoring the schema at N-1. The framework removes N's schema version record in the same transaction as the downgrade; SQL bodies must not manage version records.

`make test` runs `TestMigrationDowngradesRequired` and `TestMigrationDowngradeSchemaParity` for every registered migration above 42. The parity test migrates separate empty fixture databases to N and N-1, downgrades the N database, and compares the logical `sqlite_master` schema and recorded version. It excludes physical root pages, orders objects by type and name, and trims outer SQL whitespace without changing literals. A test-only migration exercises the framework before the first production migration above 42 lands.

This check proves schema parity on fixtures. It does not guarantee preservation of application data or expose an operator downgrade command. Existing migrations through V42 are exempt; the boundary stays fixed when the supported version increases.
