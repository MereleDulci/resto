package req

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestAuth struct {
	ID string
}

func (ta *TestAuth) Clone() Clonable {
	return &TestAuth{}
}

func TestCtx_Clone(t *testing.T) {
	t.Parallel()

	t.Run("clone should be thread safe", func(t *testing.T) {
		auth := &TestAuth{
			ID: "foo",
		}
		source := MakeNewCtx()
		source.SetAuthentication(auth)

		derivative := source.Derive()

		derivative.Authentication().(*TestAuth).ID = "bar"
		assert.Equal(t, source.Authentication().(*TestAuth).ID, "foo", "expected source to not be modified")
		assert.Equal(t, derivative.Authentication().(*TestAuth).ID, "bar", "expected derivative to be modified")
	})
}
