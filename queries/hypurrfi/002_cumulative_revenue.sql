-- Name: HypurrFi + Competitors — Cumulative Revenue
-- Purpose: Running total revenue by protocol for HypurrFi and competitors.
-- Inputs: protocol list
-- Output columns: day, protocol, revenue_usd_daily, revenue_usd_cumulative
-- Source tables: none
-- APIs: https://api.llama.fi/protocol/ 
-- Dune URL: https://dune.com/queries/5933759/9578730

WITH params AS (
  SELECT DATE '2025-02-18' AS start_date
),
revenue_protocols AS (
  SELECT * FROM (VALUES ('hypurrfi'), ('hyperlend'), ('felix'), ('felix-cdp')) AS t(slug)
),
rev_raw AS (
  SELECT
    slug,
    TRY(json_parse(http_get(concat('https://api.llama.fi/summary/fees/', slug, '?dataType=dailyRevenue')))) AS j
  FROM revenue_protocols
),
rev_top AS (
  SELECT
    slug,
    CAST(from_unixtime(CAST(json_extract_scalar(x, '$[0]') AS bigint)) AS date) AS day,
    CAST(json_extract_scalar(x, '$[1]') AS double) AS revenue_usd
  FROM rev_raw
  CROSS JOIN UNNEST(CAST(json_extract(j, '$.totalDataChart') AS array(json))) AS t(x)
  WHERE j IS NOT NULL
),
rev_bd_rows AS (
  SELECT slug, row_json
  FROM rev_raw
  CROSS JOIN UNNEST(CAST(json_extract(j, '$.totalDataChartBreakdown') AS array(json))) AS u(row_json)
  WHERE j IS NOT NULL
),
rev_bd_points AS (
  SELECT
    slug,
    CAST(from_unixtime(CAST(json_extract_scalar(row_json, '$[0]') AS bigint)) AS date) AS day,
    COALESCE(
      REDUCE(
        map_values(
          TRY_CAST(
            COALESCE(
              json_extract(row_json, '$[1]["Hyperliquid L1"]'),
              json_extract(row_json, '$[1]["Hyperliquid"]')
            ) AS map(varchar, double)
          )
        ),
        CAST(0 AS double),
        (s, v) -> s + v,
        s -> s
      ),
      TRY_CAST(json_extract_scalar(row_json, '$[1]["Hyperliquid L1"]') AS double),
      TRY_CAST(json_extract_scalar(row_json, '$[1]["Hyperliquid"]') AS double),
      0.0
    ) AS revenue_usd
  FROM rev_bd_rows
),
rev_union AS (
  SELECT slug, day, revenue_usd FROM rev_top
  UNION ALL
  SELECT slug, day, revenue_usd FROM rev_bd_points
),
rev_daily AS (
  SELECT
    CASE WHEN slug IN ('felix','felix-cdp') THEN 'felix' ELSE slug END AS slug,
    day,
    SUM(revenue_usd) AS revenue_usd
  FROM rev_union
  GROUP BY 1,2
),
filtered AS (
  SELECT slug, day, revenue_usd
  FROM rev_daily
  WHERE day >= (SELECT start_date FROM params)
)
SELECT
  slug,
  day,
  revenue_usd,
  SUM(revenue_usd) OVER (
    PARTITION BY slug ORDER BY day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_revenue_usd
FROM filtered
ORDER BY slug, day;