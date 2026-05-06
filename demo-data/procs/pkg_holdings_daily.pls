-- Package: WH_DW.PKG_HOLDINGS_DAILY
-- Purpose: nightly holding-fact load + portfolio performance refresh.
CREATE OR REPLACE PACKAGE BODY WH_DW.PKG_HOLDINGS_DAILY AS

  PROCEDURE load_fact_holding_daily(p_run_dt IN DATE) IS
  BEGIN
    INSERT INTO WH_DW.FACT_HOLDING_DAILY (
      holding_sk, account_sk, member_sk, fund_sk, date_key,
      unit_count, unit_price, market_value_amt, src_sys, load_dt
    )
    SELECT
      WH_DW.SEQ_HOLDING_SK.NEXTVAL,
      a.account_sk,
      a.member_sk,
      f.fund_sk,
      TO_NUMBER(TO_CHAR(h.asof_dt, 'YYYYMMDD')) AS date_key,
      h.unit_count,
      h.unit_price,
      h.market_value_amt,
      h.src_sys,
      SYSTIMESTAMP
    FROM WH_STG.STG_HOLDING h
    JOIN WH_DW.DIM_ACCOUNT a ON a.account_natural_key = h.account_natural_key AND a.current_flg = 'Y'
    JOIN WH_DW.DIM_FUND f ON f.fund_code = h.fund_code
    WHERE h.asof_dt = p_run_dt;

    COMMIT;
  END load_fact_holding_daily;


  PROCEDURE load_fact_account_balance(p_run_dt IN DATE) IS
  BEGIN
    MERGE INTO WH_DW.FACT_ACCOUNT_DAILY_BALANCE tgt
    USING (
      SELECT
        h.account_sk,
        a.member_sk,
        h.date_key,
        SUM(h.market_value_amt) AS market_value_amt,
        SUM(h.unit_count)       AS units_total,
        MAX(h.src_sys)          AS src_sys
      FROM WH_DW.FACT_HOLDING_DAILY h
      JOIN WH_DW.DIM_ACCOUNT a ON a.account_sk = h.account_sk
      WHERE h.date_key = TO_NUMBER(TO_CHAR(p_run_dt, 'YYYYMMDD'))
      GROUP BY h.account_sk, a.member_sk, h.date_key
    ) src
    ON (tgt.account_sk = src.account_sk AND tgt.date_key = src.date_key)
    WHEN MATCHED THEN UPDATE SET
      tgt.market_value_amt = src.market_value_amt,
      tgt.units_total      = src.units_total,
      tgt.member_sk        = src.member_sk,
      tgt.load_dt          = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
      account_sk, member_sk, date_key, market_value_amt, units_total, src_sys, load_dt
    ) VALUES (
      src.account_sk, src.member_sk, src.date_key, src.market_value_amt, src.units_total, src.src_sys, SYSTIMESTAMP
    );

    COMMIT;
  END load_fact_account_balance;


  PROCEDURE refresh_rpt_portfolio_perf(p_run_dt IN DATE) IS
  BEGIN
    DELETE FROM WH_RPT.RPT_PORTFOLIO_PERFORMANCE_DAILY
     WHERE date_key = TO_NUMBER(TO_CHAR(p_run_dt, 'YYYYMMDD'));

    INSERT INTO WH_RPT.RPT_PORTFOLIO_PERFORMANCE_DAILY (
      account_sk, date_key, market_value_amt, daily_return_pct, ytd_return_pct, refresh_dt
    )
    SELECT
      cur.account_sk,
      cur.date_key,
      cur.market_value_amt,
      CASE WHEN prev.market_value_amt IS NULL OR prev.market_value_amt = 0 THEN NULL
           ELSE (cur.market_value_amt - prev.market_value_amt) / prev.market_value_amt * 100 END,
      CASE WHEN ytd.market_value_amt IS NULL OR ytd.market_value_amt = 0 THEN NULL
           ELSE (cur.market_value_amt - ytd.market_value_amt) / ytd.market_value_amt * 100 END,
      SYSTIMESTAMP
    FROM WH_DW.FACT_ACCOUNT_DAILY_BALANCE cur
    LEFT JOIN WH_DW.FACT_ACCOUNT_DAILY_BALANCE prev
           ON prev.account_sk = cur.account_sk AND prev.date_key = cur.date_key - 1
    LEFT JOIN WH_DW.FACT_ACCOUNT_DAILY_BALANCE ytd
           ON ytd.account_sk = cur.account_sk
          AND ytd.date_key = TO_NUMBER(TO_CHAR(TRUNC(p_run_dt, 'YEAR'), 'YYYYMMDD'))
    WHERE cur.date_key = TO_NUMBER(TO_CHAR(p_run_dt, 'YYYYMMDD'));

    COMMIT;
  END refresh_rpt_portfolio_perf;

END PKG_HOLDINGS_DAILY;
/
