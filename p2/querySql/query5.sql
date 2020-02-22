--  query5
SELECT count(*) store
FROM (SELECT
        store,
        EXTRACT(YEAR FROM weekdate)  AS year,
        EXTRACT(MONTH FROM weekdate) AS month,
        count(DISTINCT dept)            deptcnt
      FROM hw2.sales
      WHERE EXTRACT(YEAR FROM weekdate) IN ('2010', '2011', '2012')
      GROUP BY store, EXTRACT(YEAR FROM weekdate), EXTRACT(MONTH FROM weekdate)) a
  JOIN (SELECT
          store,
          count(DISTINCT dept) deptcnt
        FROM hw2.sales
        GROUP BY store) b ON a.store = b.store AND a.deptcnt = b.deptcnt
GROUP BY a.store, a.year
HAVING count(DISTINCT a.month) = 12;

-- result:
--
-- store
-- -------
-- (0 rows)

