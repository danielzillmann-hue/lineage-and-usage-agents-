-- ──────────────────────────────────────────────────────────────────────
--  Snowflake daily balances pipeline.
--
--  Anatomy:
--    1. STREAM on RAW.MEMBERS captures CDC rows since the last consumption.
--    2. Three TASKs run on a DAG (extract → enrich → publish), each
--       chained via the AFTER clause.
--    3. The middle task wraps a Snowflake-Scripting stored procedure
--       so we have something procedural to parse.
--
--  Run cadence: every 30 minutes (root task SCHEDULE), child tasks fire
--  immediately after their parents complete.
-- ──────────────────────────────────────────────────────────────────────

CREATE OR REPLACE STREAM RAW.MEMBERS_CDC ON TABLE RAW.MEMBERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE;

-- ──────────────────────────────────────────────────────────────────────
--  Task 1 (root): extract changed members into a staging table.
-- ──────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK ETL.TASK_EXTRACT_MEMBERS
    WAREHOUSE = WH_ETL
    SCHEDULE  = '30 MINUTE'
AS
    MERGE INTO STG.MEMBERS_DELTA tgt
    USING (
        SELECT
            m.member_id,
            m.fund_code,
            m.first_name,
            m.last_name,
            m.modified_at,
            m.METADATA$ACTION   AS cdc_action
        FROM RAW.MEMBERS_CDC m
        WHERE m.METADATA$ACTION IN ('INSERT', 'UPDATE')
    ) src
       ON tgt.member_id = src.member_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.fund_code   = src.fund_code,
            tgt.first_name  = src.first_name,
            tgt.last_name   = src.last_name,
            tgt.modified_at = src.modified_at
    WHEN NOT MATCHED THEN
        INSERT (member_id, fund_code, first_name, last_name, modified_at)
        VALUES (src.member_id, src.fund_code, src.first_name, src.last_name, src.modified_at);


-- ──────────────────────────────────────────────────────────────────────
--  Task 2: enrich balances by joining holdings to the member delta.
--  Wraps a Snowflake Scripting procedure so the DAG step has a body.
-- ──────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE ETL.SP_ENRICH_BALANCES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Wipe yesterday's enrichment.
    DELETE FROM STG.BALANCES_ENRICHED WHERE as_of_dt = CURRENT_DATE();

    -- Repopulate from delta + holdings.
    INSERT INTO STG.BALANCES_ENRICHED (
        member_id, fund_code, total_balance, holding_count, as_of_dt
    )
    SELECT
        d.member_id,
        d.fund_code,
        SUM(h.balance_amt)    AS total_balance,
        COUNT(h.holding_id)   AS holding_count,
        CURRENT_DATE()        AS as_of_dt
    FROM STG.MEMBERS_DELTA d
    JOIN RAW.HOLDINGS  h
      ON h.member_id = d.member_id
    GROUP BY d.member_id, d.fund_code;

    RETURN 'OK';
END;
$$;

CREATE OR REPLACE TASK ETL.TASK_ENRICH_BALANCES
    WAREHOUSE = WH_ETL
    AFTER ETL.TASK_EXTRACT_MEMBERS
AS
    CALL ETL.SP_ENRICH_BALANCES();


-- ──────────────────────────────────────────────────────────────────────
--  Task 3: publish to the reporting schema (SCD-1 upsert).
-- ──────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK ETL.TASK_PUBLISH_BALANCES
    WAREHOUSE = WH_ETL
    AFTER ETL.TASK_ENRICH_BALANCES
AS
    MERGE INTO RPT.MEMBER_BALANCE tgt
    USING STG.BALANCES_ENRICHED   src
       ON tgt.member_id = src.member_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.fund_code      = src.fund_code,
            tgt.total_balance  = src.total_balance,
            tgt.holding_count  = src.holding_count,
            tgt.as_of_dt       = src.as_of_dt
    WHEN NOT MATCHED THEN
        INSERT (member_id, fund_code, total_balance, holding_count, as_of_dt)
        VALUES (src.member_id, src.fund_code, src.total_balance,
                src.holding_count, src.as_of_dt);

-- Tasks are created suspended; resume the leaves first, then the root.
ALTER TASK ETL.TASK_PUBLISH_BALANCES RESUME;
ALTER TASK ETL.TASK_ENRICH_BALANCES  RESUME;
ALTER TASK ETL.TASK_EXTRACT_MEMBERS  RESUME;
