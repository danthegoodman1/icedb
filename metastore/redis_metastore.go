package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/danthegoodman1/GoAPITemplate/utils"
	"github.com/danthegoodman1/icedb/part"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"os"
	"time"
)

type (
	RedisMetaStore struct {
		client *redis.Client
	}
)

func NewRedisMetaStore(ctx context.Context) (*RedisMetaStore, error) {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("connecting to redis metastore")
	rms := &RedisMetaStore{
		client: redis.NewClient(&redis.Options{
			Addr:        os.Getenv("REDIS_ADDR"),
			Password:    os.Getenv("REDIS_PASSWORD"),
			DB:          0,
			DialTimeout: time.Second * 3,
		}),
	}

	// Ping test first to ensure valid connection
	if os.Getenv("REDIS_PING_TEST") == "1" {
		logger.Debug().Msg("running redis ping test")
		s := time.Now()
		_, err := rms.client.Ping(ctx).Result()
		if err != nil {
			rms.client.Close()
			return nil, fmt.Errorf("error pinging redis: %w", err)
		}
		logger.Debug().Msgf("redis ping test successful in %s", time.Since(s))
	}

	return rms, nil
}

func (rms *RedisMetaStore) TableKey(tableName string) string {
	return "t_" + tableName
}

func (rms *RedisMetaStore) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("getting table schema")
	ts := TableSchema{}
	rawTableSchema, err := rms.client.Get(ctx, rms.TableKey(table)).Result()
	if err != nil {
		return ts, fmt.Errorf("error in redis GET: %w", err)
	}

	// Bind JSON string to struct
	err = json.Unmarshal([]byte(rawTableSchema), &ts)
	if err != nil {
		return ts, fmt.Errorf("error in json.Unmarshall: %w", err)
	}

	return ts, nil
}

func (rms *RedisMetaStore) CreateTableSchema(
	ctx context.Context,
	tableName string,
	partKeyColNames,
	partKeyColTypes,
	orderingKeyColNames,
	orderingKeyColTypes []string,
) error {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("creating table schema")
	tableID := utils.GenRandomShortID()
	ts := TableSchema{
		ID:                   tableID,
		Name:                 tableName,
		PartitionKeyColNames: partKeyColNames,
		PartitionKeyColTypes: partKeyColTypes,
		OrderingKeyColNames:  orderingKeyColNames,
		OrderingKeyColTypes:  orderingKeyColTypes,
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}

	jsonBytes, err := json.Marshal(ts)
	if err != nil {
		return fmt.Errorf("error in json.Marshal: %w", err)
	}

	_, err = rms.client.SetNX(ctx, rms.TableKey(tableName), string(jsonBytes), 0).Result()
	if err != nil {
		return fmt.Errorf("error in redis SETNX: %w", err)
	}

	return nil
}

func (rms *RedisMetaStore) ListParts(ctx context.Context, table string) ([]part.Part, error) {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("listing parts")
	rawParts, err := rms.client.HGetAll(ctx, rms.TableKey(table)+"_parts").Result()
	if err != nil {
		return nil, fmt.Errorf("error in redis HGETALL: %w", err)
	}

	parts := make([]part.Part, len(rawParts))
	for partID, rawJSON := range rawParts {
		part := part.Part{}
		err = json.Unmarshal([]byte(rawJSON), &part)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling part ID '%s' under table '%s': %w", partID, table, err)
		}
		parts = append(parts, part)
	}

	return parts, nil
}

func (rms *RedisMetaStore) Shutdown(_ context.Context) error {
	err := rms.client.Close()
	if err != nil {
		return fmt.Errorf("error closing redis client: %w", err)
	}
	return nil
}
