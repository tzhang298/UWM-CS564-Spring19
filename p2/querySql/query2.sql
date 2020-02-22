--  query2
SELECT hw2.stores.store
FROM hw2.stores
  JOIN (SELECT a.store
        FROM (SELECT DISTINCT (store) store
              FROM hw2.temporaldata
              WHERE unemploymentrate > 10) a
          JOIN (SELECT store
                FROM hw2.temporaldata
                GROUP BY store
                HAVING max(fuelprice) < 4) b ON a.store = b.store) c ON hw2.stores.store = c.store;

-- result:
-- store
-- -------
-- 34
-- 43
-- (2 rows)
--

