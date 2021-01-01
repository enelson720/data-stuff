WITH ranking AS (
    SELECT
        start_time
      , end_time
      , query_text
      , replace(lower(regexp_substr(query_text, '"ANALYTICS".{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1, 'si')),
                '__dbt_tmp', '')                                                                                      AS short_query
      , query_type
      , credits_used_cloud_services + CASE
                                          WHEN warehouse_name = 'TRANSFORM_L'                   THEN 8
                                          WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS') THEN 1
                                          WHEN warehouse_name = 'REPORTING_M'                   THEN 4
                                                                                                ELSE 1 END *
                                      ((total_elapsed_time / 60000) / 60)                                             AS credits_used_cloud_services
      , sum(rows_produced) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS rows_produced
      , sum(rows_unloaded) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS rows_unloaded
      , sum(rows_inserted) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS rows_inserted
      , sum(rows_updated) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS rows_updated
      , sum(rows_deleted) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS rows_deleted
      , warehouse_name
      , sum(total_elapsed_time) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS total_elapsed_time
      , CASE
            WHEN warehouse_name = 'TRANSFORM_L'                   THEN 8
            WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS') THEN 1
            WHEN warehouse_name = 'REPORTING_M'                   THEN 4
                                                                  ELSE 1 END *
        ((total_elapsed_time / 60000) / 60)                                                                           AS compute_credits_used
      , min(CASE
                WHEN query_type = 'CREATE_TABLE_AS_SELECT' THEN (credits_used_cloud_services + CASE
                                                                                                   WHEN warehouse_name = 'TRANSFORM_L'
                                                                                                       THEN 8
                                                                                                   WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS')
                                                                                                       THEN 1
                                                                                                   WHEN warehouse_name = 'REPORTING_M'
                                                                                                       THEN 4
                                                                                                       ELSE 1 END *
                                                                                               ((total_elapsed_time / 60000) / 60))
                                                           ELSE NULL END)
            OVER (PARTITION BY start_time::DATE, database_name--, query_type--, user_name
                , replace(lower(regexp_substr(query_text,
                                              '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}',
                                              1, 1,
                                              'si')),
                          '__dbt_tmp',
                          '') )                                                                                       AS min_credits_used_daily
      , max((credits_used_cloud_services + CASE
                                               WHEN warehouse_name = 'TRANSFORM_L'                   THEN 8
                                               WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS') THEN 1
                                               WHEN warehouse_name = 'REPORTING_M'                   THEN 4
                                                                                                     ELSE 1 END *
                                           ((total_elapsed_time / 60000) / 60))) OVER (PARTITION BY database_name--, query_type--, user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  '') )                                                                                               AS rank
      , row_number() OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp', '') ORDER BY (credits_used_cloud_services + CASE
                                                                               WHEN warehouse_name = 'TRANSFORM_L'
                                                                                   THEN 8
                                                                               WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS')
                                                                                   THEN 1
                                                                               WHEN warehouse_name = 'REPORTING_M'
                                                                                   THEN 4
                                                                                   ELSE 1 END *
                                                                           ((total_elapsed_time / 60000) / 60)) DESC) AS day_rank
      , SUM(CASE
                WHEN warehouse_name = 'TRANSFORM_L'                   THEN 8
                WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS') THEN 1
                WHEN warehouse_name = 'REPORTING_M'                   THEN 4
                                                                      ELSE 1 END * (total_elapsed_time / 60000 / 60))
            OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
                , replace(lower(regexp_substr(query_text,
                                              '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}',
                                              1, 1, 'si')),
                          '__dbt_tmp', '')) +
        sum(credits_used_cloud_services) OVER (PARTITION BY start_time::DATE, database_name--, query_type--,user_name
            , replace(lower(regexp_substr(query_text,
                                          '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}',
                                          1, 1, 'si')),
                      '__dbt_tmp',
                      ''))                                                                                            AS day_sum
      , SUM(CASE
                WHEN warehouse_name = 'TRANSFORM_L'                   THEN 8
                WHEN warehouse_name IN ('TRANSFORM_XS', 'ANALYST_XS') THEN 1
                WHEN warehouse_name = 'REPORTING_M'                   THEN 4
                                                                      ELSE 1 END * (total_elapsed_time / 60000 / 60))
            OVER (PARTITION BY database_name--, query_type--,user_name
                , replace(lower(regexp_substr(query_text,
                                              '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}',
                                              1, 1, 'si')),
                          '__dbt_tmp', '')) + sum(credits_used_cloud_services) OVER (PARTITION BY database_name--, query_type--,user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  ''))                                                                                                AS alltime_sum
      , NiceBytes(max(bytes_scanned) OVER (PARTITION BY start_time::DATE, database_name--, query_type--, user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  '') ))                                                                                              AS max_bytes_daily
      , NiceBytes(sum(bytes_scanned) OVER (PARTITION BY database_name--, query_type--, user_name
        , replace(lower(regexp_substr(query_text,
                                      '("ANALYTICS"|ANALYTICS|analytics).{1}[A-Za-z_]{0,100}.{1}[A-Za-z_]{0,100}', 1, 1,
                                      'si')),
                  '__dbt_tmp',
                  '') ))                                                                                              AS max_bytes_alltime
    FROM snowflake.account_usage.query_history
    WHERE database_name = 'ANALYTICS'
      AND query_type NOT IN ('SELECT')
      --AND execution_status = 'SUCCESS'
      AND warehouse_name != 'STITCH'
      AND start_time >= '2020-12-01'
                )
SELECT
    start_time::DATE                                                                                 AS start_date
  , end_time::DATE                                                                                   AS end_date
  , short_query                                                                                      AS query
  , credits_used_cloud_services                                                                      AS max_credits_used_daily
  , min_credits_used_daily
  , rank                                                                                             AS max_credits_used_alltime
  , day_sum                                                                                          AS credits_used_daily
  , day_sum * 2.58                                                                                   AS cash_money_daily
  , alltime_sum * 2.58                                                                               AS cash_money_alltime
  , rank = credits_used_cloud_services                                                               AS most_expensivest
  , max_bytes_daily
  , max_bytes_alltime                                                                                AS sum_bytes_daily
  , total_elapsed_time                                                                               AS elapsed_time_daily
  , rows_produced
  , rows_deleted
  , rows_updated
  , rows_inserted
  , warehouse_name
  , dense_rank()
            OVER (PARTITION BY date_trunc(MONTH, start_time::DATE) ORDER BY cash_money_alltime DESC) AS expensivest_ranking
  , sum(cash_money_daily) OVER ()                                                                    AS current_monthly_cost_all_jobs
FROM ranking
WHERE day_rank = 1
ORDER BY cash_money_alltime DESC NULLS LAST, start_time DESC;