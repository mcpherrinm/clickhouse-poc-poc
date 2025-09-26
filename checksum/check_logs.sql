WITH CRC32(Body) as crc
SELECT
    Body,
    crc,
    LogAttributes.checksum as checksum,
    base64URLEncode(
        arrayStringConcat(
            arrayMap(x -> char(x),
                arrayMap(i ->
                    if(i = 1, -- For the first byte
                        if(crc >= 128, -- If there's more bytes
                            bitOr(crc, 128), -- Include the continuation high bit
                            crc
                        ),
                    if(i = 2, -- Subsequent bytes are similar, but shifted by 7
                        if(bitShiftRight(crc, 7) >= 128,
                            bitOr(bitAnd(bitShiftRight(crc, 7), 127), 128),
                            if(bitShiftRight(crc, 7) > 0, bitAnd(bitShiftRight(crc, 7), 127), 0)
                        ),
                    if(i = 3,
                        if(bitShiftRight(crc, 14) >= 128,
                            bitOr(bitAnd(bitShiftRight(crc, 14), 127), 128),
                            if(bitShiftRight(crc, 14) > 0, bitAnd(bitShiftRight(crc, 14), 127), 0)
                        ),
                    if(i = 4,
                        if(bitShiftRight(crc, 21) >= 128,
                            bitOr(bitAnd(bitShiftRight(crc, 21), 127), 128),
                            if(bitShiftRight(crc, 21) > 0, bitAnd(bitShiftRight(crc, 21), 127), 0)
                        ),
                    if(i = 5,
                        if(bitShiftRight(crc, 28) > 0, bitAnd(bitShiftRight(crc, 28), 127), 0),
                        0
                  )))))
                , range(1, 6))
             )
        )
    ) as computed
FROM otel.otel_logs
WHERE LogAttributes.checksum IS NOT NULL AND checksum != computed
ORDER BY crc;
