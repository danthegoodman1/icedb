package main

import (
	"context"
	"fmt"
	"github.com/danthegoodman1/icedb/partitioner"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danthegoodman1/icedb/crdb"
	"github.com/danthegoodman1/icedb/gologger"
	"github.com/danthegoodman1/icedb/http_server"
	"github.com/danthegoodman1/icedb/utils"
)

var logger = gologger.NewLogger()

func main() {
	logger.Debug().Msg("starting IceDB 🧊")

	if err := crdb.ConnectToDB(); err != nil {
		logger.Error().Err(err).Msg("error connecting to CRDB")
		os.Exit(1)
	}

	// err := migrations.CheckMigrations(utils.CRDB_DSN)
	// if err != nil {
	// 	logger.Error().Err(err).Msg("Error checking migrations")
	// 	os.Exit(1)
	// }

	httpServer := http_server.StartHTTPServer()

	// Register partition functions
	partitioner.RegisterFunctions()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	logger.Warn().Msg("received shutdown signal!")

	// For AWS ALB needing some time to de-register pod
	// Convert the time to seconds
	sleepTime := utils.GetEnvOrDefaultInt("SHUTDOWN_SLEEP_SEC", 0)
	logger.Info().Msg(fmt.Sprintf("sleeping for %ds before exiting", sleepTime))

	time.Sleep(time.Second * time.Duration(sleepTime))
	logger.Info().Msg(fmt.Sprintf("slept for %ds, exiting", sleepTime))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to shutdown HTTP server")
	} else {
		logger.Info().Msg("successfully shutdown HTTP server")
	}
}
