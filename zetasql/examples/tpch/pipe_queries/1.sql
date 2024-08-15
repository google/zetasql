# A simple aggregate query with grouping and ordering collapses
# to just an AGGREGATE call using GROUP AND ORDER BY shorthand.
FROM lineitem
|> WHERE l_shipdate <= date_sub(date '1998-12-01', INTERVAL 74 day)
|> AGGREGATE
     sum(l_quantity) AS sum_qty,
     sum(l_extendedprice) AS sum_base_price,
     sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
     avg(l_quantity) AS avg_qty,
     avg(l_extendedprice) AS avg_price,
     avg(l_discount) AS avg_disc,
     COUNT(*) AS count_order
   GROUP
     AND ORDER BY
       l_returnflag,
     l_linestatus;
