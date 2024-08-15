FROM
  customer,
  orders,
  lineitem,
  (
    FROM lineitem
    |> AGGREGATE sum(l_quantity) AS sum_quantity
       GROUP BY l_orderkey AS selected_l_orderkey
    |> WHERE sum_quantity > 230
  )
|> WHERE
     o_orderkey = selected_l_orderkey
     AND c_custkey = o_custkey
     AND o_orderkey = l_orderkey
|> AGGREGATE sum(l_quantity)
   GROUP BY
     c_name,
     c_custkey,
     o_orderkey,
     o_orderdate,
     o_totalprice
|> ORDER BY
     o_totalprice DESC,
     o_orderdate
|> LIMIT 100;
