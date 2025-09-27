package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

// varintEncode matches what Boulder does to encode the crc log line checksums
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
