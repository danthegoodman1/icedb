package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/danthegoodman1/icedb/part"
	"github.com/danthegoodman1/icedb/utils"
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
	partKeyOps,
	orderingKeyOps []part.KeyOp,
) error {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("creating table schema")
	tableID := utils.GenRandomShortID()
	ts := TableSchema{
		ID:             tableID,
		Name:           tableName,
		PartKeyOps:     partKeyOps,
		OrderingKeyOps: orderingKeyOps,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
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

func (rms *RedisMetaStore) ListParts(ctx context.Context, table string, filters ...FilterOption) ([]part.Part, error) {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msgf("listing parts with filter options %+v", filters)

	var cursorPos uint64 = 0
	var returnedCursor uint64 = 1
	parts := make([]part.Part, 0)

	// Loop until we have all the results
	for returnedCursor != 0 {
		logger.Debug().Msgf("running redis HSCAN with cursor %d", cursorPos)
		rawParts, newCursor, err := rms.client.HScan(ctx, rms.TableKey(table)+"_parts", cursorPos, "", 0).Result()
		if err != nil {
			return nil, fmt.Errorf("error in redis HGETALL: %w", err)
		}

	AllParts:
		for partID, rawJSON := range rawParts {
			part := part.Part{}
			err = json.Unmarshal([]byte(rawJSON), &part)
			if err != nil {
				return nil, fmt.Errorf("error unmarshalling part ID '%s' under table '%s': %w", partID, table, err)
			}
			if !part.Alive {
				continue
			}

			// Verify against filter options
			for _, filter := range filters {
				if !rms.PassFilterOption(part.ID, filter) {
					// If any fail, we skip this part
					continue AllParts
				}
			}
			parts = append(parts, part)
		}

		returnedCursor = newCursor
		cursorPos = newCursor
	}

	return parts, nil
}

func (rms *RedisMetaStore) CreatePart(ctx context.Context, table string, p part.Part, colMarks []part.ColumnMark) error {
	pipe := rms.client.TxPipeline()

	partJSON, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("error json.Marshal(part): %w", err)
	}

	// Insert part
	pipe.HSet(ctx, rms.TableKey(table)+"_parts", p.ID, string(partJSON))

	// Build a single HSet of all column marks
	colMarksHash := make([]any, len(colMarks))
	for _, colMark := range colMarks {
		jsonBytes, err := json.Marshal(colMark)
		if err != nil {
			return fmt.Errorf("error in json.Marshal(colMark): %w", err)
		}
		colMarksHash = append(colMarksHash, colMark.ColumnName, string(jsonBytes))
	}

	// Insert column marks
	pipe.HSet(ctx, rms.TableKey(table)+"_part_"+p.ID, colMarksHash...)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error in redis pipeline exec: %w", err)
	}

	return nil
}

func (rms *RedisMetaStore) Shutdown(_ context.Context) error {
	err := rms.client.Close()
	if err != nil {
		return fmt.Errorf("error closing redis client: %w", err)
	}
	return nil
}

func (rms *RedisMetaStore) PassFilterOption(val string, filter FilterOption) bool {
	switch filter.Operator {
	case GT:
		return val > filter.Val.(string)
	case GTE:
		return val >= filter.Val.(string)
	case IN:
		return utils.ContainsString(filter.Val.([]string), val)
	case LT:
		return val < filter.Val.(string)
	case LTE:
		return val <= filter.Val.(string)
	default:
		return false
	}
}
