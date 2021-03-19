/*
    3. Provide the list of canceled cards and how many days each of them was active.

    What's the definition of canceled card?
        * Definition 1: A card with status = 'REQUEST_CANCELED'. I have reject
        this definition, because those cards map to card_instance.status =
        'program_terminated'. But there are other reasons for a
        deactivated_card, namely 'reported_lost', 'support_terminated'.
        * Definition 2: A card instance where deactivation_reason is not null.

    I have used Definition 2.

    Question: can a card be reactivated after an activation?
*/

WITH

latest_card_instances AS (
    SELECT DISTINCT
            card_id,
            FIRST_VALUE(creation_date) OVER w AS creation_date,
            FIRST_VALUE(deactivation_date) OVER w AS deactivation_date
    FROM card_instance
    WINDOW w AS (PARTITION BY card_id ORDER BY creation_date DESC)
),

deactivated_cards AS (
    SELECT
        card_id,
        deactivation_date,
        creation_date,
        DATEDIFF(deactivation_date, creation_date) AS n_days_active
    FROM latest_card_instances
    WHERE deactivation_date IS NOT NULL
)

SELECT *
FROM deactivated_cards
