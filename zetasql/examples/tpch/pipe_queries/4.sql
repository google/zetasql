# The simple outer aggregate involved repeating columns across
# SELECT, GROUP BY and ORDERY.  In pipe syntax, all of that is
# just once with AGGREGATE, using GROUP AND ORDER BY.
# Also, the EXISTS subquery is written without a placeholder SELECT list.
FROM
  orders
|> WHERE
     o_orderdate >= date '1997-06-01'
     AND o_orderdate < date_add(date '1997-06-01', INTERVAL 3 month)
     AND EXISTS(
       FROM lineitem
       |> WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
|> AGGREGATE COUNT(*) AS order_count
   GROUP AND ORDER BY o_orderpriority;
