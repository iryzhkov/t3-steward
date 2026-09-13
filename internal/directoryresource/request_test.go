package directoryresource

import "testing"

func TestRequestAmbiguityAndAccessAreRejected(t *testing.T) {
	valid := Request{WorkerID: "host", ResourceID: "data", Revision: "1"}
	for name, requests := range map[string][]Request{
		"duplicate":         {valid, valid},
		"different workers": {valid, {WorkerID: "other", ResourceID: "other-data", Revision: "1"}},
		"invalid access":    {{WorkerID: "host", ResourceID: "data", Revision: "1", Access: "write"}},
		"blank revision":    {{WorkerID: "host", ResourceID: "data"}},
		"untrimmed worker":  {{WorkerID: " host", ResourceID: "data", Revision: "1"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateRequests(requests); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
	if err := ValidateRequests([]Request{valid}); err != nil {
		t.Fatal(err)
	}
}
