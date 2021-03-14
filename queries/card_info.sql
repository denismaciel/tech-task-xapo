/*

1. Provide the following information for each card: order date, issue date,
activation date, first used date.
    * Ordered date: when the card was created => `card.creation_date`
    * Issue date: when the (first) card (instance) was shipped =>
      `card_instance_shipping.creation_date where status = 'shipped'`
    * activation date: card instance activated => `card_instance.activation_date`
    * first used date: date of first transatcion in transactions
*/

WITH 

ordered_date AS (
    SELECT 
        id AS card_id,
        creation_date AS ordered_date
    FROM card
),

/*
    The issued_date of a card is considered to be the issued_date of its first
    instance.
*/
card_first_instance AS (
    SELECT DISTINCT
        card_id,
        FIRST_VALUE(id) OVER w AS card_instance_id
    FROM card_instance
    WINDOW w AS (PARTITION BY card_id ORDER BY creation_date)
),

issued_date AS (
    SELECT
        card_id,
        MIN(creation_date) AS issued_date
    FROM card_instance_shipping
    LEFT JOIN card_first_instance
        USING (card_instance_id)
    WHERE status = 'shipped'
    GROUP BY card_id
),

activation_date AS (
    SELECT
        card_id,
        MIN(activation_date) AS activation_date
    FROM card_instance
    GROUP BY card_id
),

first_used_date AS (
    SELECT
        card_id,
        MIN(tx_date) AS first_used_date
    FROM transactions
    GROUP BY card_id
),

joined AS (
    SELECT 
        ordered_date.card_id,
        ordered_date.ordered_date,
        issued_date.issued_date,
        activation_date.activation_date,
        first_used_date.first_used_date
    FROM ordered_date
    LEFT JOIN issued_date
        USING (card_id)
    LEFT JOIN activation_date
        USING (card_id)
    LEFT JOIN first_used_date
        USING (card_id)
)

SELECT *
FROM joined
