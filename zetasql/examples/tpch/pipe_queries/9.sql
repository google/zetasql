# Removes unnecessary subquery and internal SELECT and aliasing of
# n_name column by using EXTEND to compute expressions.
FROM
  part,
  supplier,
  lineitem,
  partsupp,
  orders,
  nation
|> WHERE
     s_suppkey = l_suppkey
     AND ps_suppkey = l_suppkey
     AND ps_partkey = l_partkey
     AND p_partkey = l_partkey
     AND o_orderkey = l_orderkey
     AND s_nationkey = n_nationkey
     AND p_name LIKE '%tomato%'
|> EXTEND
     EXTRACT(year FROM o_orderdate) AS o_year,
     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
|> AGGREGATE
     sum(amount) AS sum_profit
   GROUP BY
     n_name AS nation ASC,
     o_year DESC;
