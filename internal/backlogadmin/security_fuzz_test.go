package backlogadmin

import (
	"bytes"
	"testing"
)

func FuzzLocalAdminFrameStrictBoundedDecode(f *testing.F) {
	f.Add([]byte{0, 0, 0, 2, '{', '}'})
	f.Add([]byte{0, 0, 4, 1, '{'})
	f.Fuzz(func(t *testing.T, frame []byte) {
		var request localRequest
		_ = readLocalJSON(bytes.NewReader(frame), 1024, &request)
	})
}
