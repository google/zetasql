# Removes a subquery in favor of linear flow, with SELECT to compute
# expressions before the AGGREGATE.
# The final SELECT is unnecessary since it just selects the grouping and
# aggregate columns with AGGREGATE already produces.
FROM
  supplier,
  lineitem,
  orders,
  customer,
  nation n1,
  nation n2
|> WHERE
     s_suppkey = l_suppkey
     AND o_orderkey = l_orderkey
     AND c_custkey = o_custkey
     AND s_nationkey = n1.n_nationkey
     AND c_nationkey = n2.n_nationkey
     AND (
       (n1.n_name = 'SAUDI ARABIA' AND n2.n_name = 'UNITED KINGDOM')
       OR (n1.n_name = 'UNITED KINGDOM' AND n2.n_name = 'SAUDI ARABIA'))
     AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31'
|> SELECT
     n1.n_name AS supp_nation,
     n2.n_name AS cust_nation,
     EXTRACT(year FROM l_shipdate) AS l_year,
     l_extendedprice * (1 - l_discount) AS volume
|> AGGREGATE
     sum(volume) AS revenue
   GROUP
     AND ORDER BY
       supp_nation,
     cust_nation,
     l_year;
