package utils

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/danthegoodman1/GoAPITemplate/gologger"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/segmentio/ksuid"
)

var logger = gologger.NewLogger()

func GetEnvOrDefault(env, defaultVal string) string {
	e := os.Getenv(env)
	if e == "" {
		return defaultVal
	} else {
		return e
	}
}

func GetEnvOrDefaultInt(env string, defaultVal int64) int64 {
	e := os.Getenv(env)
	if e == "" {
		return defaultVal
	} else {
		intVal, err := strconv.ParseInt(e, 10, 16)
		if err != nil {
			logger.Error().Msg(fmt.Sprintf("Failed to parse string to int '%s'", env))
			os.Exit(1)
		}

		return (intVal)
	}
}

func GenRandomID(prefix string) string {
	return prefix + gonanoid.MustGenerate("abcdefghijklmonpqrstuvwxyzABCDEFGHIJKLMONPQRSTUVWXYZ0123456789", 22)
}

func GenKSortedID(prefix string) string {
	return prefix + ksuid.New().String()
}

func GenRandomShortID() string {
	// reduced character set that's less probable to mis-type
	// change for conflicts is still only 1:128 trillion
	return gonanoid.MustGenerate("abcdefghikmonpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 8)
}

func Ptr[T any](s T) *T {
	return &s
}

func Deref[T any](ref *T, fallback T) T {
	if ref == nil {
		return fallback
	}
	return *ref
}

func ArrayOrEmpty[T any](ref []T) []T {
	if ref == nil {
		return make([]T, 0)
	}
	return ref
}

var ErrVersionBadFormat = PermError("bad version format")

// VersionToInt converts a simple semantic version string (e.e. 18.02.66)
func VersionToInt(v string) (int64, error) {
	sParts := strings.Split(v, ".")
	if len(sParts) > 3 {
		return -1, ErrVersionBadFormat
	}
	var iParts = make([]int64, 3)
	for i := range sParts {
		vp, err := strconv.ParseInt(sParts[i], 10, 64)
		if err != nil {
			return -1, fmt.Errorf("error in ParseInt: %s %w", err.Error(), ErrVersionBadFormat)
		}
		iParts[i] = vp
	}
	return iParts[0]*10_000*10_000 + iParts[1]*10_000 + iParts[2], nil
}

func ContainsString(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
