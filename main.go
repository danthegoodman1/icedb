package main

import (
	"github.com/danthegoodman1/GoAPITemplate/gologger"
)

var (
	logger = gologger.NewLogger()
)

func main() {
	logger.Debug().Msg("hello world")
}
