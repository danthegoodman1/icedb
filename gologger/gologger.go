package gologger

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type ctxKey string

const ReqIDKey ctxKey = "reqID"

func init() {
	l := NewLogger()
	zerolog.DefaultContextLogger = &l
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		function := ""
		fun := runtime.FuncForPC(pc)
		if fun != nil {
			funName := fun.Name()
			slash := strings.LastIndex(funName, "/")
			if slash > 0 {
				funName = funName[slash+1:]
			}
			function = " " + funName + "()"
		}
		return file + ":" + strconv.Itoa(line) + function
	}
}

func NewLogger() zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	// zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.TimestampFieldName = "time"

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	logger = logger.Hook(CallerHook{})

	if os.Getenv("PRETTY") == "1" {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	if os.Getenv("DEBUG") == "1" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	return logger
}

type CallerHook struct{}

func (h CallerHook) Run(e *zerolog.Event, _ zerolog.Level, _ string) {
	e.Caller(3)
}
