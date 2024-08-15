SELECT
  cntrycode,
  COUNT(*) AS numcust,
  sum(c_acctbal) AS totacctbal
FROM
  (
    SELECT
      substr(c_phone, 1, 2) AS cntrycode,
      c_acctbal
    FROM customer
    WHERE
      substr(c_phone, 1, 2) IN ('10', '19', '14', '22', '23', '31', '13')
      AND c_acctbal > (
        SELECT avg(c_acctbal)
        FROM customer
        WHERE
          c_acctbal > 0.00
          AND substr(c_phone, 1, 2) IN ('10', '19', '14', '22', '23', '31', '13')
      )
      AND NOT EXISTS(
        SELECT *
        FROM orders
        WHERE o_custkey = c_custkey
      )
  ) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;
