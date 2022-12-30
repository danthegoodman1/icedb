package partitioner

import (
	"fmt"
	"testing"
	"time"
)

func TestToDay(t *testing.T) {
	RegisterFunctions()

	f := Functions["toDay"]

	day, err := f(map[string]any{"hey": "ho"}, []string{"now()"})
	if err != nil {
		t.Fatal(err)
	}

	if day != fmt.Sprint(time.Now().Day()) {
		t.Fatal("mismatched date")
	}

	day, err = f(map[string]any{"t": "2022-01-24T00:00:00.000Z"}, []string{"t"})
	if err != nil {
		t.Fatal(err)
	}

	if day != "24" {
		t.Fatal("mismatched date for t string")
	}

	day, err = f(map[string]any{"t": 1672406408279.0}, []string{"t"})
	if err != nil {
		t.Fatal(err)
	}

	if day != "30" {
		t.Fatal("mismatched date for t int")
	}

	_, err = f(map[string]any{"t": 1672406408279}, []string{"t"})
	if err != ErrInvalidColumnType {
		t.Fatal("did not get invalid col type")
	}
}
