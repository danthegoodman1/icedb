package partitioner

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type (
	PartitionPlan struct {
		Func string
		Args []string
		As   string
	}

	PartitionFunc func(row map[string]any, args []string) (string, error)
)

var (
	Functions = make(map[string]PartitionFunc)

	ErrFuncNotFound = errors.New("partition function not found")

	ErrMissingArgs       = errors.New("missing args")
	ErrMissingColumns    = errors.New("missing one or more columns specified in args")
	ErrInvalidColumnType = errors.New("invalid column type")
)

func RegisterFunctions() {
	Functions["toDay"] = func(row map[string]any, args []string) (string, error) {
		t, err := parseTimeFuncfunc(row, args)
		if err != nil {
			return "", fmt.Errorf("error in parseTimeFunc: %w", err)
		}

		return fmt.Sprint(t.Day()), nil
	}
	Functions["toMonth"] = func(row map[string]any, args []string) (string, error) {
		t, err := parseTimeFuncfunc(row, args)
		if err != nil {
			return "", fmt.Errorf("error in parseTimeFunc: %w", err)
		}

		return fmt.Sprint(t.Month()), nil
	}
	Functions["toYear"] = func(row map[string]any, args []string) (string, error) {
		t, err := parseTimeFuncfunc(row, args)
		if err != nil {
			return "", fmt.Errorf("error in parseTimeFunc: %w", err)
		}

		return fmt.Sprint(t.Year()), nil
	}
	Functions["toYearDay"] = func(row map[string]any, args []string) (string, error) {
		t, err := parseTimeFuncfunc(row, args)
		if err != nil {
			return "", fmt.Errorf("error in parseTimeFunc: %w", err)
		}

		return fmt.Sprint(t.YearDay()), nil
	}
	Functions["toYearWeek"] = func(row map[string]any, args []string) (string, error) {
		t, err := parseTimeFuncfunc(row, args)
		if err != nil {
			return "", fmt.Errorf("error in parseTimeFunc: %w", err)
		}

		return fmt.Sprint(t.ISOWeek()), nil
	}
	Functions["toWeekDay"] = func(row map[string]any, args []string) (string, error) {
		t, err := parseTimeFuncfunc(row, args)
		if err != nil {
			return "", fmt.Errorf("error in parseTimeFunc: %w", err)
		}

		return fmt.Sprint(t.Weekday()), nil
	}
}

func GetRowPartition(row map[string]any, partitioners []PartitionPlan) (string, error) {
	var finalParts []string
	for _, partFunc := range partitioners {
		f, ok := Functions[partFunc.Func]
		if !ok {
			return "", ErrFuncNotFound
		}

		s, err := f(row, partFunc.Args)
		if err != nil {
			return "", fmt.Errorf("error processing partition function %s: %w", partFunc.Func, err)
		}
		finalParts = append(finalParts, fmt.Sprintf("%s=%s", partFunc.As, s))
	}
	return strings.Join(finalParts, "/"), nil
}

func parseTimeFuncfunc(row map[string]any, args []string) (t time.Time, err error) {
	if len(args) == 0 {
		err = ErrMissingArgs
		return
	}

	key := args[0]

	if key == "now()" {
		t = time.Now()
	} else {
		value, exists := row[key]
		if !exists {
			err = ErrMissingColumns
			return
		}

		var err error
		if valString, isStr := value.(string); isStr {
			// We have a datetime like YYYY-MM-DDTHH:mm:ss.sssZ
			t, err = time.Parse("2006-01-02T15:04:05.000Z", valString)
			if err != nil {
				err = fmt.Errorf("error in time.Parse for string: %w", err)
				return
			}
		} else if valFloat, isFloat := value.(float64); isFloat {
			// We have a float as an int
			t = time.UnixMilli(int64(valFloat))
		} else {
			err = ErrInvalidColumnType
			return
		}
	}
	return
}
