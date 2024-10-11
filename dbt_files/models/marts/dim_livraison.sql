WITH commentaire AS (
    SELECT
        c.customername,
        o.orderNumber,
        o.orderdate,
        o.status,
        DATEDIFF(day, o.orderDate, o.requiredDate) AS delais_expedition,
        DATEDIFF(day, o.orderDate, o.shippedDate) AS delais_expedition_realiser,
        o.comments
    FROM
        {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_customers') }} c ON o.customerNumber = c.customerNumber
)

SELECT
    *,
    CASE
        WHEN delais_expedition < delais_expedition_realiser THEN 'Retard de livraison'
        WHEN delais_expedition > delais_expedition_realiser THEN 'Livraison en avance'
        WHEN delais_expedition = delais_expedition_realiser THEN 'Livraison dans les temps'
        ELSE 'Livraison non réalisée'
    END AS status_livraison
FROM
    commentaire