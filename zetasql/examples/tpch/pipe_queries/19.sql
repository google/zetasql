FROM
  lineitem,
  part
|> WHERE
     # Added this because optimizer is needed to pull this out of the OR.
     p_partkey = l_partkey
     AND (
       (
         p_partkey = l_partkey
         AND p_brand = 'Brand#53'
         # and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
         AND l_quantity >= 5
         AND l_quantity <= 5 + 10
         AND p_size BETWEEN 1 AND 5
         # and l_shipmode in ('AIR', 'AIR REG')
         AND l_shipinstruct = 'DELIVER IN PERSON')
       OR (
         p_partkey = l_partkey
         AND p_brand = 'Brand#41'
         # and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
         AND l_quantity >= 15
         AND l_quantity <= 15 + 10
         AND p_size BETWEEN 1 AND 10
         # and l_shipmode in ('AIR', 'AIR REG')
         AND l_shipinstruct = 'DELIVER IN PERSON')
       OR (
         p_partkey = l_partkey
         AND p_brand = 'Brand#21'
         # and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
         AND l_quantity >= 29
         AND l_quantity <= 29 + 10
         AND p_size BETWEEN 1 AND 15
         # and l_shipmode in ('AIR', 'AIR REG')
         AND l_shipinstruct = 'DELIVER IN PERSON'))
|> AGGREGATE
     sum(l_extendedprice * (1 - l_discount)) AS revenue;
