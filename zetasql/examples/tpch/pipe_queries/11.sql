# HAVING is replaced by just a WHERE after the aggregate.
# It can use the `value` column rather than repeating the aggregate expression.
FROM
  partsupp,
  supplier,
  nation
|> WHERE
     ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'PERU'
|> AGGREGATE sum(ps_supplycost * ps_availqty) AS value
   GROUP BY ps_partkey
|> WHERE
     value > (
       FROM
         partsupp,
         supplier,
         nation
       |> WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'PERU'
       |> AGGREGATE sum(ps_supplycost * ps_availqty) * 0.0001000000)
|> ORDER BY value DESC;
