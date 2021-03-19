WITH 

dedup_txs AS (
    SELECT DISTINCT
        id,
        card_id,
        transaction_amount,
        transaction_currency,
        status
    FROM transactions
)

SELECT
    card_id,
    SUM(transactions.transaction_amount / usd_rates.dollar) AS total_spent
FROM dedup_txs AS transactions
LEFT JOIN usd_rates
    ON transactions.transaction_currency = usd_rates.symbol
WHERE status = 'completed'
GROUP BY card_id
ORDER BY total_spent DESC
LIMIT 10
