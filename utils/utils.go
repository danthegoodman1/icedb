package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/danthegoodman1/GoAPITemplate/gologger"
	"github.com/labstack/echo/v4"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/rs/zerolog"
	"github.com/segmentio/ksuid"

	"github.com/UltimateTournament/backoff/v4"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
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

// Cannot use to set something to "", must manually use sq.NullString for that
func SQLNullString(s string) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  s != "",
	}
}

func SQLNullStringP(s *string) sql.NullString {
	return sql.NullString{
		String: Deref(s, ""),
		Valid:  s != nil,
	}
}

func SQLNullInt64P(s *int64) sql.NullInt64 {
	return sql.NullInt64{
		Int64: Deref(s, 0),
		Valid: s != nil,
	}
}

func SQLNullBoolP(s *bool) sql.NullBool {
	return sql.NullBool{
		Bool:  Deref(s, false),
		Valid: s != nil,
	}
}

// Cannot use to set something to 0, must manually use sq.NullInt64 for that
func SQLNullInt64(s int64) sql.NullInt64 {
	return sql.NullInt64{
		Int64: s,
		Valid: true,
	}
}

func GenRandomShortID() string {
	// reduced character set that's less probable to mis-type
	// change for conflicts is still only 1:128 trillion
	return gonanoid.MustGenerate("abcdefghikmonpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 8)
}

func DaysUntil(t time.Time, d time.Weekday) int {
	delta := d - t.Weekday()
	if delta < 0 {
		delta += 7
	}
	return int(delta)
}

// this wrapper exists so caller stack skipping works
func ReliableExec(ctx context.Context, pool *pgxpool.Pool, tryTimeout time.Duration, f func(ctx context.Context, conn *pgxpool.Conn) error) error {
	return reliableExec(ctx, pool, tryTimeout, func(ctx context.Context, tx *pgxpool.Conn) error {
		return f(ctx, tx)
	})
}

func ReliableExecInTx(ctx context.Context, pool *pgxpool.Pool, tryTimeout time.Duration, f func(ctx context.Context, conn pgx.Tx) error) error {
	return reliableExec(ctx, pool, tryTimeout, func(ctx context.Context, tx *pgxpool.Conn) error {
		return crdbpgx.ExecuteTx(ctx, tx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return f(ctx, tx)
		})
	})
}

func reliableExec(ctx context.Context, pool *pgxpool.Pool, tryTimeout time.Duration, f func(ctx context.Context, conn *pgxpool.Conn) error) error {
	cfg := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)

	return backoff.RetryNotify(func() error {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return backoff.Permanent(err)
			}
			return err
		}
		defer conn.Release()
		tryCtx, cancel := context.WithTimeout(ctx, tryTimeout)
		defer cancel()
		err = f(tryCtx, conn)
		if errors.Is(err, pgx.ErrNoRows) {
			return backoff.Permanent(err)
		}
		if IsPermSQLErr(err) {
			return backoff.Permanent(err)
		}
		// not context.DeadlineExceeded as that's expected due to `tryTimeout`
		if errors.Is(err, context.Canceled) {
			return backoff.Permanent(err)
		}
		return err
	}, cfg, func(err error, d time.Duration) {
		reqID, _ := ctx.Value(gologger.ReqIDKey).(string)
		l := zerolog.Ctx(ctx).Info().Err(err).CallerSkipFrame(5)
		if reqID != "" {
			l.Str(string(gologger.ReqIDKey), reqID)
		}
		l.Msg("ReliableExec retrying")
	})
}

func IsPermSQLErr(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			// This is a duplicate key - unique constraint
			return true
		}
		if pgErr.Code == "42703" {
			// Column does not exist
			return true
		}
	}
	return false
}

func IsUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			// This is a duplicate key - unique constraint
			return true
		}
	}
	return false
}

func Ptr[T any](s T) *T {
	return &s
}

type NoEscapeJSONSerializer struct{}

var _ echo.JSONSerializer = &NoEscapeJSONSerializer{}

func (d *NoEscapeJSONSerializer) Serialize(c echo.Context, i interface{}, indent string) error {
	enc := json.NewEncoder(c.Response())
	enc.SetEscapeHTML(false)
	if indent != "" {
		enc.SetIndent("", indent)
	}
	return enc.Encode(i)
}

// Deserialize reads a JSON from a request body and converts it into an interface.
func (d *NoEscapeJSONSerializer) Deserialize(c echo.Context, i interface{}) error {
	// Does not escape <, >, and ?
	err := json.NewDecoder(c.Request().Body).Decode(i)
	var ute *json.UnmarshalTypeError
	var se *json.SyntaxError
	if ok := errors.As(err, &ute); ok {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Unmarshal type error: expected=%v, got=%v, field=%v, offset=%v", ute.Type, ute.Value, ute.Field, ute.Offset)).SetInternal(err)
	} else if ok := errors.As(err, &se); ok {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Syntax error: offset=%v, error=%v", se.Offset, se.Error())).SetInternal(err)
	}
	return err
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

var emptyJSON = pgtype.JSONB{Bytes: []byte("{}"), Status: pgtype.Present}

func OrEmptyJSON(data pgtype.JSONB) pgtype.JSONB {
	if data.Status == pgtype.Null {
		data = emptyJSON
	}
	return data
}

func IfElse[T any](check bool, a T, b T) T {
	if check {
		return a
	}
	return b
}

func OrEmptyArray[T any](a []T) []T {
	if a == nil {
		return make([]T, 0)
	}
	return a
}

func FirstOr[T any](a []T, def T) T {
	if len(a) == 0 {
		return def
	}
	return a[0]
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
