package http_server

import (
	"context"
	"fmt"
	"github.com/danthegoodman1/icedb/crdb"
	"github.com/danthegoodman1/icedb/query"
	"github.com/danthegoodman1/icedb/utils"
	"github.com/jackc/pgx/v4/pgxpool"
	"net/http"
	"time"
)

type (
	column struct {
		Name string
		// string, number, or list(x)
		Type string
	}
)

func (s *HTTPServer) GetColumnsForNamespace(c *CustomContext) error {
	ns := c.Param("ns")

	var columns []column
	err := utils.ReliableExec(c.Request().Context(), crdb.PGPool, time.Second*15, func(ctx context.Context, conn *pgxpool.Conn) error {
		cols, err := query.New(conn).GetColumns(ctx, ns)
		if err != nil {
			return fmt.Errorf("error in GetColumnNames: %w", err)
		}
		for _, col := range cols {
			columns = append(columns, column{
				Name: col.Col,
				Type: col.Type,
			})
		}
		return nil
	})
	if err != nil {
		return c.InternalError(err, "error getting columns")
	}

	return c.JSON(http.StatusOK, columns)
}

func (s *HTTPServer) GetNamespaces(c *CustomContext) error {
	var namespaces []string
	err := utils.ReliableExec(c.Request().Context(), crdb.PGPool, time.Second*15, func(ctx context.Context, conn *pgxpool.Conn) (err error) {
		namespaces, err = query.New(conn).ListNamespaces(ctx)
		return
	})
	if err != nil {
		return c.InternalError(err, "error getting columns")
	}

	return c.JSON(http.StatusOK, namespaces)
}
