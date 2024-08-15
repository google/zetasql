# A subquery isn't needed to compute an expression column so it
# can be referenced multiple times.
# It can just be computed directly in GROUP BY and given an alias.
FROM customer
|> WHERE
     substr(c_phone, 1, 2) IN ('10', '19', '14', '22', '23', '31', '13')
     AND c_acctbal > (
       SELECT avg(c_acctbal)
       FROM customer
       WHERE
         c_acctbal > 0.00
         AND substr(c_phone, 1, 2) IN ('10', '19', '14', '22', '23', '31', '13')
     )
     AND NOT EXISTS(
       FROM orders
       |> WHERE o_custkey = c_custkey
     )
|> AGGREGATE
     COUNT(*) AS numcust,
     sum(c_acctbal) AS totacctbal
   GROUP AND ORDER BY substr(c_phone, 1, 2) AS cntrycode;
