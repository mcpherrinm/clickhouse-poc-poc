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

	// Create the varintEncode function in ClickHouse
	if err := client.createVarintEncodeFunction(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create varintEncode function: %w", err)
	}

	return client, nil
}

// Close closes the ClickHouse connection
func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}

// createVarintEncodeFunction creates the varintEncode function in ClickHouse
func (c *ClickHouseClient) createVarintEncodeFunction() error {
	createFunctionSQL := `
CREATE OR REPLACE FUNCTION varintEncode AS (input_value) -> (
    SELECT 
        -- Map over the 5 output bytes in
        arrayStringConcat(arrayMap(x -> char(x), arrayMap(i ->
            if(bitShiftRight(input_value, 7 * i) > 0x7F, -- If there are more bytes:
                bitOr(bitAnd(bitShiftRight(input_value, 7 * i), 0x7F), 0x80), -- set the top continuation bit
                bitAnd(bitShiftRight(input_value, 7 * i), 0x7F) -- otherwise shift out 7 bits
            )
        , range(0, 5))))
)`

	_, err := c.conn.ExecContext(c.ctx, createFunctionSQL)
	return err
}

// VarintEncode computes varintEncode for a single value using ClickHouse
func (c *ClickHouseClient) VarintEncode(value uint32) (string, error) {
	var result string
	query := fmt.Sprintf("SELECT base64URLEncode(varintEncode(%d))", value)
	err := c.conn.QueryRowContext(c.ctx, query).Scan(&result)
	if err != nil {
		return "", fmt.Errorf("failed to query ClickHouse for value %d: %w", value, err)
	}
	return result, nil
}
