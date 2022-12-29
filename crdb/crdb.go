package crdb

import (
	"context"
	"time"

	"github.com/danthegoodman1/GoAPITemplate/gologger"
	"github.com/danthegoodman1/GoAPITemplate/utils"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	PGPool                 *pgxpool.Pool
	StandardContextTimeout = 10 * time.Second

	logger = gologger.NewLogger()
)

func ConnectToDB() error {
	logger.Debug().Msg("connecting to CRDB...")
	var err error
	config, err := pgxpool.ParseConfig(utils.CRDB_DSN)
	if err != nil {
		return err
	}

	config.MaxConns = 10
	config.MinConns = 1
	config.HealthCheckPeriod = time.Second * 5
	config.MaxConnLifetime = time.Minute * 30
	config.MaxConnIdleTime = time.Minute * 30

	PGPool, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return err
	}
	logger.Debug().Msg("connected to CRDB")
	return nil
}
