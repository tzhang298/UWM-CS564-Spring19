--  query10
SELECT
  tmp1.year              yr,
  cast(tmp1.qtr AS TEXT) qtr,
  max(CASE type
      WHEN 'A'
        THEN sales END)  store_a_sales,
  max(CASE type
      WHEN 'B'
        THEN sales END)  store_b_sales,
  max(CASE type
      WHEN 'C'
        THEN sales END)  store_c_sales
FROM (SELECT
        EXTRACT(YEAR FROM weekdate)    AS year,
        EXTRACT(QUARTER FROM weekdate) AS qtr,
        type,
        sum(weeklysales)                  sales
      FROM hw2.sales
        JOIN hw2.stores ON hw2.sales.store = hw2.stores.store
      GROUP BY EXTRACT(YEAR FROM weekdate), EXTRACT(QUARTER FROM weekdate), type
      ORDER BY EXTRACT(YEAR FROM weekdate), EXTRACT(QUARTER FROM weekdate), type) tmp1
GROUP BY tmp1.year, tmp1.qtr
UNION ALL SELECT
            tmp1.yr,
            ' ' AS             qtr,
            sum(store_a_sales) store_a_sales,
            sum(store_b_sales) store_b_sales,
            sum(store_c_sales) store_c_sales
          FROM (SELECT
                  tmp1.year             yr,
                  tmp1.qtr,
                  max(CASE type
                      WHEN 'A'
                        THEN sales END) store_a_sales,
                  max(CASE type
                      WHEN 'B'
                        THEN sales END) store_b_sales,
                  max(CASE type
                      WHEN 'C'
                        THEN sales END) store_c_sales
                FROM (SELECT
                        EXTRACT(YEAR FROM weekdate)    AS year,
                        EXTRACT(QUARTER FROM weekdate) AS qtr,
                        type,
                        sum(weeklysales)                  sales
                      FROM hw2.sales
                        JOIN hw2.stores ON hw2.sales.store = hw2.stores.store
                      GROUP BY EXTRACT(YEAR FROM weekdate), EXTRACT(QUARTER FROM weekdate), type
                      ORDER BY EXTRACT(YEAR FROM weekdate), EXTRACT(QUARTER FROM weekdate),
                        type) tmp1
                GROUP BY tmp1.year, tmp1.qtr) tmp1
          GROUP BY tmp1.yr
ORDER BY yr;

-- result:
--
-- yr  | qtr | store_a_sales | store_b_sales | store_c_sales
-- ------+-----+---------------+---------------+---------------
-- 2010 | 1   |   2.38155e+08 |   1.11852e+08 |   2.22457e+07
-- 2010 | 2   |   3.90789e+08 |    1.8321e+08 |   3.63698e+07
-- 2010 | 3   |   3.82692e+08 |   1.78504e+08 |   3.62903e+07
-- 2010 | 4   |   4.53791e+08 |   2.16413e+08 |   3.85719e+07
-- 2010 |     |   1.46543e+09 |   6.89978e+08 |   1.33478e+08
-- 2011 |     |   1.57821e+09 |   7.24119e+08 |   1.45873e+08
-- 2011 | 1   |   3.41851e+08 |   1.53904e+08 |   3.36366e+07
-- 2011 | 2   |   3.85808e+08 |   1.75556e+08 |   3.65837e+07
-- 2011 | 3   |   4.13364e+08 |   1.87497e+08 |   3.84974e+07
-- 2011 | 4   |   4.37185e+08 |   2.07162e+08 |   3.71554e+07
-- 2012 | 1   |   3.81453e+08 |     1.725e+08 |   3.85172e+07
-- 2012 | 2   |   3.98116e+08 |   1.81932e+08 |   3.82483e+07
-- 2012 | 3   |   3.89167e+08 |   1.78201e+08 |   3.76367e+07
-- 2012 | 4   |    1.1864e+08 |   5.39715e+07 |   1.17501e+07
-- 2012 |     |   1.28738e+09 |   5.86604e+08 |   1.26152e+08
-- (15 rows)


