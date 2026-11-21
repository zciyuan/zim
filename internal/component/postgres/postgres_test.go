package postgres

import "testing"

func TestDisabledPool(t *testing.T) {
	component := New()
	if component.Name() != "postgres" {
		t.Fatalf("Name = %q", component.Name())
	}
	if component.Pool() != nil {
		t.Fatal("expected nil pool without a DSN")
	}
	component.Start()
	component.Close()
}
