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
    WITH
        arrayMap(i ->
            if(i = 1, -- For the first byte
                if(input_value >= 128, -- If there's more bytes
                    bitOr(bitAnd(input_value, 127), 128), -- Include the continuation high bit
                    input_value
                ),
            if(i = 2,
                if(bitShiftRight(input_value, 7) >= 128,
                    bitOr(bitAnd(bitShiftRight(input_value, 7), 127), 128),
                    if(bitShiftRight(input_value, 7) > 0, bitAnd(bitShiftRight(input_value, 7), 127), 0)
                ),
            if(i = 3,
                if(bitShiftRight(input_value, 14) >= 128,
                    bitOr(bitAnd(bitShiftRight(input_value, 14), 127), 128),
                    if(bitShiftRight(input_value, 14) > 0, bitAnd(bitShiftRight(input_value, 14), 127), 0)
                ),
            if(i = 4,
                if(bitShiftRight(input_value, 21) >= 128,
                    bitOr(bitAnd(bitShiftRight(input_value, 21), 127), 128),
                    if(bitShiftRight(input_value, 21) > 0, bitAnd(bitShiftRight(input_value, 21), 127), 0)
                ),
            if(i = 5,
                if(bitShiftRight(input_value, 28) > 0, bitAnd(bitShiftRight(input_value, 28), 127), 0),
                0
            )))))
        , range(1, 6)) AS varint_bytes

    SELECT arrayStringConcat(arrayMap(x -> char(x), varint_bytes))
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
