SELECT
  sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
  lineitem,
  part
WHERE
  p_partkey = l_partkey
  AND p_brand = 'Brand#33'
  AND p_container = 'LG DRUM'
  AND l_quantity < (
    SELECT
      0.2 * avg(l_quantity)
    FROM
      lineitem
    WHERE
      l_partkey = p_partkey
  );
