/*
    2. Provide the list of cards which took more than 3 days to be activated
    and more than 10 days to be used for the first time. Include the
    corresponding dates.

    Assumption:
        * I am using the issued_date as a reference date to compute both how
        long it took for the card activation and for its first usage.
*/

WITH

filter_cards AS (
    SELECT
        card_id,
        activation_date,
        first_used_date
    FROM card_info
    WHERE
        DATEDIFF(activation_date, issued_date) > 3
        AND DATEDIFF(first_used_date, issued_date) > 10
        AND activation_date IS NOT NULL
        AND first_used_date IS NOT NULL
)

SELECT *
FROM filter_cards
