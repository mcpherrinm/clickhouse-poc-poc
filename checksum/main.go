package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
)

// LogLineChecksum is the "new" boulder algorithm
func LogLineChecksum(line string) string {
	crc := crc32.ChecksumIEEE([]byte(line))
	buf := make([]byte, crc32.Size)
	_, _ = binary.Encode(buf, binary.LittleEndian, crc)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func main() {
	args := strings.Join(os.Args[1:], " ")
	fmt.Printf("%s %s\n", LogLineChecksum(args), args)
}
