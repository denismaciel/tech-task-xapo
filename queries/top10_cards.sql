SELECT
    card_id,
    SUM(transactions.transaction_amount / usd_rates.dollar) AS total_spent
FROM transactions
LEFT JOIN usd_rates
    ON transactions.transaction_currency = usd_rates.symbol
WHERE status = 'completed' 
GROUP BY card_id
ORDER BY total_spent DESC
LIMIT 10
