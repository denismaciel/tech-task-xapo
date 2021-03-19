WITH
/*
    Task 4: Provide the list of merchants together with the number of transactions they had.
    Task 5: Provide the transaction counts by status and by merchant.
*/

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
ORDER BY n DESC
