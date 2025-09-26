package main

import (
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// FuzzVarintEncode compares the Go varintEncode function with the ClickHouse SQL function using random uint32 values
func FuzzVarintEncode(f *testing.F) {
	// Create ClickHouse client
	client, err := NewClickHouseClient()
	if err != nil {
		f.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	defer client.Close()

	// Add some seed values to ensure we test known edge cases
	seedValues := []uint32{
		0,
		1,
		127,
		128,
		16384,
		179340738,
		3632233996,
		1668494043,
		2147483647,
		4294967295, // max uint32
	}

	for _, seed := range seedValues {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, value uint32) {
		// Get Go function result
		goResult := varintEncode(value)

		// Get ClickHouse function result
		clickhouseResult, err := client.VarintEncode(value)
		if err != nil {
			t.Fatalf("Failed to get ClickHouse result for value %d: %v", value, err)
		}

		// Compare results
		if goResult != clickhouseResult {
			t.Errorf("Results differ for value %d:\n  Go result:         %s\n  ClickHouse result: %s",
				value, goResult, clickhouseResult)
		}
	})
}
