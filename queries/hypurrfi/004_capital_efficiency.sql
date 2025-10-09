-- Name: HypurrFi + Competitors — Capital Efficiency (7D/30D) Time Series
-- Purpose: Time-series Revenue/TVL for 7D & 30D windows by protocol.
-- Inputs: protocol list
-- Output columns: slug, name, ann_rev_over_tvl_7d, ann_rev_over_tvl_30d
-- Source tables: none 
-- APIs: https://api.llama.fi/protocol/ 
-- Dune URL: https://dune.com/queries/5933761/9578761

WITH params AS (
  SELECT DATE '2025-02-18' AS start_date
),

/* ----- Revenue (merges felix-cdp) ----- */
revenue_protocols AS (
  SELECT * FROM (VALUES ('hypurrfi'), ('hyperlend'), ('felix'), ('felix-cdp')) AS t(slug)
),
rev_raw AS (
  SELECT slug,
         TRY(json_parse(http_get(concat('https://api.llama.fi/summary/fees/', slug, '?dataType=dailyRevenue')))) AS j
  FROM revenue_protocols
),
rev_top AS (
  SELECT slug,
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
  SELECT slug,
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
  SELECT CASE WHEN slug IN ('felix','felix-cdp') THEN 'felix' ELSE slug END AS slug,
         day,
         SUM(revenue_usd) AS revenue_usd
  FROM rev_union
  GROUP BY 1,2
),

/* ----- TVL (Hyperliquid L1 preferred) ----- */
tvl_protocols AS (
  SELECT * FROM (VALUES ('hypurrfi'), ('hyperlend'), ('felix')) AS t(slug)
),
tvl_raw AS (
  SELECT slug,
         TRY(json_parse(http_get(concat('https://api.llama.fi/protocol/', slug)))) AS j
  FROM tvl_protocols
),
tvl_chain AS (
  SELECT slug,
         CAST(from_unixtime(CAST(json_extract_scalar(x, '$[0]') AS bigint)) AS date) AS day,
         CAST(json_extract_scalar(x, '$[1]') AS double) AS tvl_usd,
         2 AS priority
  FROM tvl_raw
  CROSS JOIN UNNEST(
    CAST(
      COALESCE(
        json_extract(j, '$.chainTvls["Hyperliquid L1"].tvl'),
        json_extract(j, '$.chainTvls["Hyperliquid"].tvl')
      ) AS array(json)
    )
  ) AS t(x)
  WHERE j IS NOT NULL
),
tvl_top AS (
  SELECT slug,
         CAST(from_unixtime(CAST(json_extract_scalar(pt, '$.date') AS bigint)) AS date) AS day,
         CAST(json_extract_scalar(pt, '$.totalLiquidityUSD') AS double) AS tvl_usd,
         1 AS priority
  FROM tvl_raw
  CROSS JOIN UNNEST(CAST(json_extract(j, '$.tvl') AS array(json))) AS u(pt)
  WHERE j IS NOT NULL
),
tvl_union AS (
  SELECT * FROM tvl_chain
  UNION ALL
  SELECT * FROM tvl_top
),
tvl_daily AS (
  SELECT slug,
         day,
         CAST(MAX_BY(tvl_usd, priority) AS double) AS tvl_usd
  FROM tvl_union
  GROUP BY 1,2
),

/* ----- Join + 7D/30D windows & ratios ----- */
joined AS (
  SELECT COALESCE(r.slug, t.slug) AS slug,
         COALESCE(r.day,  t.day)  AS day,
         r.revenue_usd,
         t.tvl_usd
  FROM rev_daily r
  FULL JOIN tvl_daily t
    ON r.slug = t.slug AND r.day = t.day
),
filtered AS (
  SELECT slug, day,
         COALESCE(revenue_usd, 0) AS revenue_usd,
         tvl_usd
  FROM joined
  WHERE day >= (SELECT start_date FROM params)
),
win AS (
  SELECT
    slug,
    day,
    revenue_usd,
    tvl_usd,
    -- 7D
    SUM(revenue_usd) OVER (
      PARTITION BY slug ORDER BY day
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rev_7d,
    AVG(tvl_usd) OVER (
      PARTITION BY slug ORDER BY day
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_tvl_7d,
    -- 30D
    SUM(revenue_usd) OVER (
      PARTITION BY slug ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rev_30d,
    AVG(tvl_usd) OVER (
      PARTITION BY slug ORDER BY day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_tvl_30d
  FROM filtered
),

-- === Snapshot wrapper: latest value per slug (one row per protocol) ===
snapshot AS (
  SELECT
    slug,
    day,
    CASE WHEN avg_tvl_7d  > 0 THEN (rev_7d  / avg_tvl_7d)  * (365.0/7.0)  END AS ann_rev_over_tvl_7d,
    CASE WHEN avg_tvl_30d > 0 THEN (rev_30d / avg_tvl_30d) * (365.0/30.0) END AS ann_rev_over_tvl_30d
  FROM win
),
latest AS (
  SELECT slug, MAX(day) AS max_day
  FROM snapshot
  GROUP BY slug
),

-- Pretty labels for charts (keeps slug intact)
labels AS (
  SELECT * FROM (VALUES
    ('hypurrfi',  'HypurrFi'),
    ('hyperlend', 'Hyperlend'),
    ('felix',     'Felix')
  ) AS t(slug, display_name)
)

SELECT
  s.slug,  -- keep for stability/joins
  COALESCE(l.display_name, INITCAP(s.slug)) AS name,
  s.ann_rev_over_tvl_7d,
  s.ann_rev_over_tvl_30d
FROM snapshot s
JOIN latest mx
  ON s.slug = mx.slug AND s.day = mx.max_day
LEFT JOIN labels l
  ON l.slug = s.slug
ORDER BY s.ann_rev_over_tvl_7d DESC;