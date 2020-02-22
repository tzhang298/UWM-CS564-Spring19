--  query3
SELECT count(*)
FROM (SELECT c.weekdate
      FROM hw2.sales c
        JOIN (SELECT weekdate
              FROM hw2.holidays
              WHERE isholiday = 'f') d ON c.weekdate = d.weekdate
      GROUP BY c.weekdate
      HAVING sum(weeklysales) > (SELECT avg(sumsales)
                                 FROM (SELECT
                                         a.weekdate,
                                         sum(weeklysales) sumsales
                                       FROM hw2.sales a
                                         JOIN (SELECT weekdate
                                               FROM hw2.holidays
                                               WHERE isholiday = 't') b ON b.weekdate = a.weekdate
                                       GROUP BY a.weekdate) avg_sale)) f;


-- result:
-- count
-- -------
-- 8
-- (1 row)

