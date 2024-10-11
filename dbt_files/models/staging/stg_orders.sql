SELECT
    orderNumber,
    customerNumber,
    orderDate,
    requiredDate,
    shippedDate,
    status,
    comments
FROM
    {{ source('raw', 'ORDERS_CLEANED') }}