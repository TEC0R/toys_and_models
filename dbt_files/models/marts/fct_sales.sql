SELECT
    o.orderNumber,
    o.orderDate,
    o.customerNumber,
    c.customerName,
    c.country as customer_country,
    od.productCode,
    od.quantityOrdered,
    od.priceEach,
    od.line_total,
    od.line_profit,
    p.productLine
FROM
    {{ ref('stg_orders') }} o
JOIN
    {{ ref('int_order_details') }} od ON o.orderNumber = od.orderNumber
JOIN
    {{ ref('stg_customers') }} c ON o.customerNumber = c.customerNumber
JOIN
    {{ ref('stg_products') }} p ON od.productCode = p.productCode