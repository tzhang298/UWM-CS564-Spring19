--  query7
SELECT
  tmp3.dept,
  tmp3.contribution avg
FROM (SELECT
        tmp1.dept,
        avg((tmp1.sum_sales / tmp2.sum_sales)) contribution
      FROM (SELECT
              store,
              dept,
              sum(weeklysales) sum_sales
            FROM hw2.sales
            GROUP BY store, dept) tmp1
        JOIN (SELECT
                store,
                sum(weeklysales) sum_sales
              FROM hw2.sales
              GROUP BY store) tmp2 ON tmp1.store = tmp2.store
      GROUP BY tmp1.dept) tmp3
  JOIN (SELECT tmp4.dept
        FROM (SELECT
                store,
                dept,
                sum(weeklysales) sum_sales
              FROM hw2.sales
              GROUP BY store, dept) tmp4
          JOIN (SELECT
                  store,
                  sum(weeklysales) sum_sales
                FROM hw2.sales
                GROUP BY store) tmp5 ON tmp4.store = tmp5.store
        WHERE (tmp4.sum_sales / tmp5.sum_sales * 100) >= 5.0
        GROUP BY tmp4.dept
        HAVING count(tmp4.dept) >= 3) tmp6 ON tmp3.dept = tmp6.dept;

-- result:
--
-- dept |        avg
-- ------+--------------------
-- 94 | 0.0304081375555446
-- 95 | 0.0695251010358334
-- 40 | 0.0441973276022408
-- 92 | 0.0730967512147294
-- 91 | 0.0313700059687512
-- 93 | 0.0254024091054592
-- 90 |  0.044952085107151
-- 38 | 0.0727544868985812
-- 2 | 0.0410644333395693
-- 72 | 0.0420093366708089
-- (10 rows)

