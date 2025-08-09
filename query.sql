-- Organized view joining minute-level status counts with auth_code 00 counts
-- Replace placeholders if running standalone; the app fills these automatically
-- {TRANSACTIONS_CSV} and {AUTH_CODES_CSV} will be injected by the app

WITH t AS (
  SELECT
    CAST(timestamp AS TIMESTAMP) AS ts,
    status,
    CAST(count AS BIGINT) AS count
  FROM read_csv_auto('{TRANSACTIONS_CSV}', header TRUE)
),
ac AS (
  SELECT
    CAST(timestamp AS TIMESTAMP) AS ts,
    auth_code,
    CAST(count AS BIGINT) AS count
  FROM read_csv_auto('{AUTH_CODES_CSV}', header TRUE)
),
a00 AS (
  SELECT ts, count AS auth_00_count
  FROM ac
  WHERE auth_code = '00'
)
SELECT
  t.ts,
  SUM(CASE WHEN t.status = 'approved' THEN t.count ELSE 0 END) AS approved,
  SUM(CASE WHEN t.status = 'denied' THEN t.count ELSE 0 END) AS denied,
  SUM(CASE WHEN t.status = 'reversed' THEN t.count ELSE 0 END) AS reversed,
  SUM(CASE WHEN t.status = 'backend_reversed' THEN t.count ELSE 0 END) AS backend_reversed,
  SUM(CASE WHEN t.status = 'failed' THEN t.count ELSE 0 END) AS failed,
  SUM(CASE WHEN t.status = 'refunded' THEN t.count ELSE 0 END) AS refunded,
  a00.auth_00_count
FROM t
LEFT JOIN a00 USING (ts)
GROUP BY t.ts, a00.auth_00_count
ORDER BY t.ts;
