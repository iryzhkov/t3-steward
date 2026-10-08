package main

// quotaHelpPages are the quota family's pages: the family, and its one verb.
func quotaHelpPages() []helpPage {
	return []helpPage{
		{Path: "quota", Body: quotaUsage},
		{
			Path:    "quota telemetry",
			Purpose: "list the quota telemetry the coordinator's recorder kept: readings, dispatch, start, finish and check events, with quota deltas for finished work.",
			Usage:   []string{"t3-steward quota telemetry [--since DUR|RFC3339] [--route TEXT] [--pool ID] [--kind K[,K]] [--limit N] [--json]"},
			Flags: []helpFlag{
				{Name: "--since", Value: "DUR|RFC3339", Default: "24h", Text: "Only events at or after this time: an age such as 6h, 90m or 7d, or an RFC3339 instant."},
				{Name: "--route", Value: "TEXT", Default: "every route", Text: "Only events whose route \"instance/model\" contains TEXT; for a reading, its bucket key."},
				{Name: "--pool", Value: "ID", Default: "every pool", Text: "Only events of this quota pool, and readings of the provider instances the configuration binds to it."},
				{Name: "--kind", Value: "K[,K]", Default: "reading,dispatch,start,finish,check,recorder", Text: "Only these event kinds, comma-separated."},
				{Name: "--limit", Value: "N", Default: "200", Text: "Keep the newest N matching events (1 to 10000); they print oldest first."},
				jsonFlag("the events and the store and recorder state"),
			},
			Exits: []helpExit{
				{0, "the store was read; the list may be empty"},
				{1, "a bad option, no store on this host (it names where to run), or a store written by a newer schema"},
			},
			JSONKeys: []string{"schemaVersion", "kind", "generatedAt", "store", "recorder", "filters", "events"},
			Notes: "It reads the recorder's file, quota-telemetry/recorder.sqlite beside the state database, read-only and " +
				"never creates it, so it answers on the coordinator host. Deltas are each window's change between the latest reading " +
				"at or before the start and the earliest at or after the finish, within 30 minutes, labelled exclusive or shared; " +
				"they are never a per-task share, and interactive or unmanaged use of the same account is not observed. Readings " +
				"that change and are overwritten on a worker between two exchanges are not seen. A chain's model-run gate task is " +
				"measured by its own finish; test durations inside one command are not captured. The task type is derived from " +
				"structured fields and labelled as such.",
			Parsers: []parserSite{{Func: "parseQuotaTelemetryArgs"}, {Func: "takeJSONFlag"}},
		},
	}
}
