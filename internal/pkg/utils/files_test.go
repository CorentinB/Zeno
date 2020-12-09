package utils

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestFileExists(t *testing.T) {
	appFS := afero.NewMemMapFs()

	afero.WriteFile(appFS, "src/a", []byte("file"), 0644)

	assert.True(t, true, FileExists("src/a"))
	assert.False(t, false, FileExists("src/b"))
}
