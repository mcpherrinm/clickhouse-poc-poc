package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"time"
)

// varintEncode encodes a uint32 value using variable-length encoding
// and returns the base64URL encoded result (without padding to match ClickHouse)
func varintEncode(value uint32) string {
	buf := make([]byte, binary.MaxVarintLen32)
	binary.PutUvarint(buf, uint64(value))
	encoded := base64.RawURLEncoding.EncodeToString(buf)
	return encoded
}

func main() {
	value := crc32.ChecksumIEEE([]byte(os.Args[1]))
	encoded := varintEncode(value)
	fmt.Printf("%d\t%s\n", value, encoded)
}

// ClickHouseClient encapsulates ClickHouse connection and varintEncode functionality
type ClickHouseClient struct {
	conn *sql.DB
	ctx  context.Context
}

// NewClickHouseClient creates a new ClickHouse client and initializes the varintEncode function
func NewClickHouseClient() (*ClickHouseClient, error) {
	conn, err := sql.Open("clickhouse", "clickhouse://default:default_user_very_bad_password@localhost:9000/otel")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ClickHouse is not ready: %w", err)
	}

	client := &ClickHouseClient{
		conn: conn,
		ctx:  context.Background(),
	}

	return client, nil
}

// Close closes the ClickHouse connection
func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}

const varintQuery = `
WITH %d as crc
SELECT
        base64URLEncode(arrayStringConcat(
                -- We map twice:
                -- First computing the CRC shifted right by increments of 7 bits
                -- Then over the shifted values, truncating at 7 bits, setting
                -- the top bit if there are following nonzero bytes.
                arrayMap(
                    shifted ->
                        char(bitOr(
                                bitAnd(shifted, 0x7F),
                                if(shifted > 0x7F, 0x80, 0x00)
                        )),
                    arrayMap(
                        i -> bitShiftRight(crc, 7 * i),
                        range(0, 5)))
        )) as computed
`

// VarintEncode computes varintEncode for a single value using ClickHouse
func (c *ClickHouseClient) VarintEncode(value uint32) (string, error) {
	var result string
	query := fmt.Sprintf(varintQuery, value)
	err := c.conn.QueryRowContext(c.ctx, query).Scan(&result)
	if err != nil {
		return "", fmt.Errorf("failed to query ClickHouse for value %d: %w", value, err)
	}
	return result, nil
}
