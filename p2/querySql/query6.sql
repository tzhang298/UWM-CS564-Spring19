--  query6
SELECT
  attribute,
  CASE WHEN correlation >= 0
    THEN '+'
  ELSE '-' END AS corr_sign,
  correlation
FROM (SELECT
        'Temperature' AS                  attribute,
        corr(temperature, t2.weeklysales) correlation
      FROM hw2.temporaldata t1
        JOIN hw2.sales t2 ON t1.store = t2.store AND t1.weekdate = t2.weekdate
      UNION SELECT
              'FuelPrice' AS                  attribute,
              corr(fuelprice, t2.weeklysales) correlation
            FROM hw2.temporaldata t1
              JOIN hw2.sales t2 ON t1.store = t2.store AND t1.weekdate = t2.weekdate
      UNION SELECT
              'CPI' AS                        attribute,
              corr(fuelprice, t2.weeklysales) correlation
            FROM hw2.temporaldata t1
              JOIN hw2.sales t2 ON t1.store = t2.store AND t1.weekdate = t2.weekdate
      UNION SELECT
              'UnemploymentRate' AS                  attribute,
              corr(unemploymentrate, t2.weeklysales) correlation
            FROM hw2.temporaldata t1
              JOIN hw2.sales t2 ON t1.store = t2.store AND t1.weekdate = t2.weekdate) t3;

-- result:
-- attribute     | corr_sign |      correlation
-- ------------------+-----------+-----------------------
-- Temperature      | -         |  -0.00231244659998809
-- CPI              | -         | -0.000120295860528548
-- UnemploymentRate | -         |   -0.0258637151104456
-- FuelPrice        | -         | -0.000120295860528548
-- (4 rows)


