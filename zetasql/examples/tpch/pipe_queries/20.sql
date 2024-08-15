# All layers of expression subqueries are rewritten to pipe syntax here,
# although this is not required.
FROM
  supplier,
  nation
|> WHERE
     s_suppkey IN (
       FROM
         partsupp,
         part
       |> WHERE p_name LIKE 'tan%'
       |> WHERE
            ps_partkey = p_partkey
            AND ps_availqty > (
              FROM lineitem
              |> WHERE
                   l_partkey = ps_partkey
                   AND l_suppkey = ps_suppkey
                   AND l_shipdate >= date '1996-01-01'
                   AND l_shipdate < date_add(date '1996-01-01', INTERVAL 1 year)
              |> AGGREGATE 0.5 * sum(l_quantity))
       |> SELECT ps_suppkey)
     AND s_nationkey = n_nationkey
     AND n_name = 'PERU'
|> SELECT s_name, s_address
|> ORDER BY s_name;
