SELECT
    Body,
    CRC32(Body) as crc,
    LogAttributes.checksum as checksum,
    base64URLEncode(arrayStringConcat( -- Map over each output byte and concat together
          arrayMap(i -> if(bitShiftRight(crc, 7 * i) > 0x7F, -- If there are more bytes:
               char(bitOr(bitAnd(bitShiftRight(crc, 7 * i), 0x7F), 0x80)), -- set the top continuation bit
               char(bitAnd(bitShiftRight(crc, 7 * i), 0x7F)) -- otherwise shift out 7 bits
            ), range(0, 5)))) as computed
FROM otel.otel_logs
WHERE LogAttributes.checksum IS NOT NULL AND checksum != computed;
