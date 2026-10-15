package extract

import (
	"context"
	"testing"
)

func TestRunEmptyScript(t *testing.T) {
	_, _, err := Run(context.Background(), "", "https://example.com")
	if err == nil {
		t.Fatalf("expected error when script is empty, got nil")
	}
}
