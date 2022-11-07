-- Data Proc base image 2.1.1 (Spark 3.2.1)
-- %spark.sql

```sql
CREATE TEMPORARY VIEW demo1_tab1m AS (
WITH tab10(a) AS ((SELECT 0 AS a) UNION ALL (SELECT 1) UNION ALL (SELECT 2) UNION ALL (SELECT 3) UNION ALL (SELECT 4) UNION ALL (SELECT 5) UNION ALL (SELECT 6) UNION ALL (SELECT 7) UNION ALL (SELECT 8) UNION ALL (SELECT 9)),
  tab1k(a) AS (SELECT ROW_NUMBER() OVER (ORDER BY x.a, y.a, z.a) FROM tab10 x, tab10 y, tab10 z)
SELECT ROW_NUMBER() OVER (ORDER BY x.a, y.a) AS num FROM tab1k x, tab1k y);
```

```sql
SELECT MIN(num), MAX(num), AVG(num) FROM demo1_tab1m
```
