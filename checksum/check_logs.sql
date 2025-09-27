SELECT
    Body,
    CRC32(Body) as crc,
    LogAttributes.checksum as checksum,
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
FROM otel.otel_logs
WHERE LogAttributes.checksum IS NOT NULL AND checksum != computed;
