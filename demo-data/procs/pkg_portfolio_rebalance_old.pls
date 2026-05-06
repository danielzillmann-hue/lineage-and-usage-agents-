-- Package: WH_LEGACY.PKG_PORTFOLIO_REBALANCE_OLD
-- Purpose: legacy MLC-era rebalance proposal generator. Replaced by Pendal engine in 2024.
-- Status:  compiled but no executions in DBA_HIST_SQLSTAT for 180+ days.
CREATE OR REPLACE PACKAGE BODY WH_LEGACY.PKG_PORTFOLIO_REBALANCE_OLD AS

  PROCEDURE generate_proposals(p_run_dt IN DATE) IS
  BEGIN
    INSERT INTO WH_LEGACY.LEG_PORTFOLIO_REBALANCE_QUEUE (
      rebalance_id, account_ref, proposed_alloc, queued_dt
    )
    SELECT
      'RBL-' || TO_CHAR(p_run_dt, 'YYYYMMDD') || '-' || ROWNUM,
      a.account_natural_key,
      'STUB',
      SYSTIMESTAMP
    FROM WH_STG.STG_ACCOUNT a
    WHERE a.status_cd = 'ACTIVE';

    INSERT INTO WH_STG.STG_PORTFOLIO_REBALANCE_TEMP (
      rebalance_id, account_natural_key, proposed_alloc, load_dt
    )
    SELECT rebalance_id, account_ref, proposed_alloc, queued_dt
      FROM WH_LEGACY.LEG_PORTFOLIO_REBALANCE_QUEUE
     WHERE queued_dt >= p_run_dt;

    COMMIT;
  END generate_proposals;

END PKG_PORTFOLIO_REBALANCE_OLD;
/
