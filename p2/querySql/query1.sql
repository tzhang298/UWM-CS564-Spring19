--  query1
SELECT
  tmp1.store,
  tmp1.hol_sales
FROM (SELECT
        store,
        SUM(weeklysales) hol_sales
      FROM hw2.sales a
        JOIN (SELECT weekdate
              FROM hw2.holidays
              WHERE isholiday = 't') b ON a.weekdate = b.weekdate
      GROUP BY a.store) tmp1
WHERE tmp1.hol_sales = (SELECT MAX(hol_sales)
                        FROM (SELECT
                                store,
                                SUM(weeklysales) hol_sales
                              FROM hw2.sales c
                                JOIN (SELECT weekdate
                                      FROM hw2.holidays
                                      WHERE isholiday = 't') d ON c.weekdate = d.weekdate
                              GROUP BY c.store) tmp2)
UNION SELECT
        tmp1.store,
        tmp1.hol_sales
      FROM (SELECT
              store,
              SUM(weeklysales) hol_sales
            FROM hw2.sales a
              JOIN (SELECT weekdate
                    FROM hw2.holidays
                    WHERE isholiday = 't') b ON a.weekdate = b.weekdate
            GROUP BY a.store) tmp1
      WHERE tmp1.hol_sales = (SELECT MIN(hol_sales)
                              FROM (SELECT
                                      store,
                                      SUM(weeklysales) hol_sales
                                    FROM hw2.sales c
                                      JOIN (SELECT weekdate
                                            FROM hw2.holidays
                                            WHERE isholiday = 't') d ON c.weekdate = d.weekdate
                                    GROUP BY c.store) tmp2);

-- result:
--
-- store |  hol_sales
-- -------+-------------
-- 33 | 2.62594e+06
-- 20 | 2.24903e+07
-- (2 rows)


