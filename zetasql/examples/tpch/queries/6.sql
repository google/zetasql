SELECT
  sum(l_extendedprice * l_discount) AS revenue
FROM
  lineitem
WHERE
  l_shipdate >= date '1994-01-01'
  AND l_shipdate < date_add(date '1994-01-01', INTERVAL 1 year)
  AND l_discount BETWEEN 0.08 - 0.01 AND 0.08 + 0.01
  AND l_quantity < 25;
