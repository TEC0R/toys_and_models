SELECT
    od.orderNumber,
    od.productCode,
    od.quantityOrdered,
    od.priceEach,
    od.orderLineNumber,
    p.productName,
    p.productLine,
    p.buyPrice,
    (od.quantityOrdered * od.priceEach) as line_total,
    (od.quantityOrdered * od.priceEach - od.quantityOrdered * p.buyPrice) as line_profit
FROM
    {{ ref('stg_orderdetails') }} od
JOIN
    {{ ref('stg_products') }} p ON od.productCode = p.productCode