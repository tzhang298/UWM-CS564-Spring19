--  query8
SELECT
  tmp2.dept,
  sum(tmp2.sales / tmp1.size) normsales
FROM (SELECT
        hw2.sales.store,
        hw2.sales.dept,
        sum(weeklysales) sales
      FROM hw2.sales
      GROUP BY hw2.sales.store, hw2.sales.dept) tmp2
  JOIN (SELECT
          hw2.sales.dept,
          hw2.sales.store,
          max(size) size
        FROM hw2.sales
          JOIN hw2.stores ON hw2.sales.store = hw2.stores.store
        GROUP BY hw2.sales.dept, hw2.sales.store) tmp1
    ON tmp2.dept = tmp1.dept AND tmp2.store = tmp1.store
GROUP BY tmp2.dept
ORDER BY sum(tmp2.sales / tmp1.size) DESC
LIMIT 10;

-- result:
-- dept |    normsales
-- ------+------------------
-- 92 | 4128.35281960417
-- 38 | 4080.21082105655
-- 95 | 3879.83498183145
-- 90 | 2567.52585804127
-- 40 | 2400.34808990609
-- 2 | 2232.72926981142
-- 72 | 2191.77411512839
-- 91 |  1791.7282519445
-- 94 | 1747.77831500644
-- 13 | 1620.50961023483
-- (10 rows)


