-- Find logs with no checksum or an incorrect checksum
SELECT
    Timestamp,
    Body,
    LogAttributes.checksum as checksum,
    base64URLEncode(reinterpretAsFixedString(CRC32(Body))) as computed
FROM otel.otel_logs
WHERE checksum IS NULL or checksum != computed
ORDER BY Timestamp desc
LIMIT 1000;

-- Count how many logs are missing a checksum, have a matching checksum, or mismatching, by day.
SELECT
    toDate(Timestamp) as date,
    LogAttributes.checksum IS NOT NULL as hasChecksum,
    base64URLEncode(reinterpretAsFixedString(CRC32(Body))) == LogAttributes.checksum as match,
    count(*)
FROM otel.otel_logs
GROUP BY ALL
ORDER BY date desc
LIMIT 1000;
