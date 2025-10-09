-- Name: HypurrFi + Competitors — TVL History
-- Purpose: Time-series TVL for HypurrFi and selected competitor protocols.
-- Inputs: protocol list 
-- Output columns: day, protocol, tvl_usd
-- Source tables: None
-- APIs: https://api.llama.fi/protocol/
-- Dune URL: https://dune.com/queries/5932268/9576906

WITH params AS (
  SELECT DATE '2025-02-18' AS start_date
),
tvl_protocols AS (
  SELECT * FROM (VALUES ('hypurrfi'), ('hyperlend'), ('felix')) AS t(slug)
),
tvl_raw AS (
  SELECT
    slug,
    TRY(json_parse(http_get(concat('https://api.llama.fi/protocol/', slug)))) AS j
  FROM tvl_protocols
),
tvl_chain AS (
  SELECT
    slug,
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
  SELECT
    slug,
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
)
SELECT
  slug,
  day,
  CAST(MAX_BY(tvl_usd, priority) AS double) AS tvl_usd
FROM tvl_union
WHERE day >= (SELECT start_date FROM params)
GROUP BY 1,2
ORDER BY slug, day;