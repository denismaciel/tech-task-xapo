WITH

remove_duplicate_txs AS (
    SELECT DISTINCT
        id,
        merchant_name,
        status
    FROM transactions
),

count_txs AS (
    SELECT
        merchant_name,
        status,
        COUNT(*) AS n
    FROM remove_duplicate_txs
    GROUP BY
        merchant_name,
        status
)

SELECT *
FROM count_txs
