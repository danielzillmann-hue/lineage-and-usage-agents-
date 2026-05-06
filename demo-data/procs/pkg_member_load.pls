-- Package: WH_DW.PKG_MEMBER_LOAD
-- Purpose: nightly load of DIM_MEMBER from STG_MEMBER (SCD2).
-- Owner:   data-platform@insignia
CREATE OR REPLACE PACKAGE BODY WH_DW.PKG_MEMBER_LOAD AS

  PROCEDURE load_dim_member(p_run_dt IN DATE) IS
  BEGIN
    -- Close existing current rows where source has changed
    UPDATE WH_DW.DIM_MEMBER d
       SET d.end_dt = p_run_dt - 1,
           d.current_flg = 'N'
     WHERE d.current_flg = 'Y'
       AND EXISTS (
         SELECT 1
           FROM WH_STG.STG_MEMBER s
          WHERE s.member_natural_key = d.member_natural_key
            AND s.src_sys = d.src_sys
            AND ( NVL(s.email, '~') <> NVL(d.email, '~')
               OR NVL(s.phone, '~') <> NVL(d.phone, '~')
               OR NVL(s.status_cd, '~') <> NVL(d.status_cd, '~')
               OR NVL(s.postcode, '~') <> NVL(d.postcode, '~')
               OR NVL(s.state_cd, '~') <> NVL(d.state_cd, '~')
            )
            AND s.load_dt > d.load_dt
       );

    -- Insert new versions (and brand-new members)
    INSERT INTO WH_DW.DIM_MEMBER (
      member_sk, member_natural_key, src_sys,
      given_name, surname, dob, email, phone, postcode, state_cd, status_cd, joined_dt,
      effective_dt, end_dt, current_flg, load_dt
    )
    SELECT WH_DW.SEQ_MEMBER_SK.NEXTVAL,
           s.member_natural_key, s.src_sys,
           s.given_name, s.surname, s.dob, s.email, s.phone, s.postcode, s.state_cd, s.status_cd, s.joined_dt,
           p_run_dt, NULL, 'Y', SYSTIMESTAMP
      FROM WH_STG.STG_MEMBER s
      LEFT JOIN WH_DW.DIM_MEMBER d
        ON d.member_natural_key = s.member_natural_key
       AND d.src_sys = s.src_sys
       AND d.current_flg = 'Y'
     WHERE d.member_sk IS NULL
        OR ( NVL(s.email, '~') <> NVL(d.email, '~')
          OR NVL(s.phone, '~') <> NVL(d.phone, '~')
          OR NVL(s.status_cd, '~') <> NVL(d.status_cd, '~')
          OR NVL(s.postcode, '~') <> NVL(d.postcode, '~')
          OR NVL(s.state_cd, '~') <> NVL(d.state_cd, '~')
           );

    COMMIT;
  END load_dim_member;


  PROCEDURE refresh_member_360(p_run_dt IN DATE) IS
  BEGIN
    DELETE FROM WH_DW.INT_MEMBER_360;

    INSERT INTO WH_DW.INT_MEMBER_360 (
      member_sk, current_account_count, total_market_value,
      ytd_contribution_amt, ytd_fee_amt, primary_adviser_sk, last_txn_dt, refresh_dt
    )
    SELECT
      m.member_sk,
      NVL(acc.cnt, 0)               AS current_account_count,
      NVL(hold.mv, 0)               AS total_market_value,
      NVL(contrib.amt, 0)           AS ytd_contribution_amt,
      NVL(fee.amt, 0)               AS ytd_fee_amt,
      adv.adviser_sk                AS primary_adviser_sk,
      txn.last_dt                   AS last_txn_dt,
      SYSTIMESTAMP                  AS refresh_dt
    FROM WH_DW.DIM_MEMBER m
    LEFT JOIN (
      SELECT member_sk, COUNT(*) AS cnt
        FROM WH_DW.DIM_ACCOUNT
       WHERE current_flg = 'Y' AND status_cd <> 'CLOSED'
       GROUP BY member_sk
    ) acc ON acc.member_sk = m.member_sk
    LEFT JOIN (
      SELECT a.member_sk, SUM(h.market_value_amt) AS mv
        FROM WH_DW.FACT_HOLDING_DAILY h
        JOIN WH_DW.DIM_ACCOUNT a ON a.account_sk = h.account_sk
       WHERE h.date_key = TO_NUMBER(TO_CHAR(p_run_dt, 'YYYYMMDD'))
       GROUP BY a.member_sk
    ) hold ON hold.member_sk = m.member_sk
    LEFT JOIN (
      SELECT t.member_sk, SUM(t.amount) AS amt
        FROM WH_DW.FACT_TRANSACTION t
        JOIN WH_DW.DIM_DATE d ON d.date_key = t.date_key
       WHERE t.txn_type_cd IN ('CONTRIB', 'EMP_CONTRIB')
         AND d.fy_year = EXTRACT(YEAR FROM p_run_dt)
       GROUP BY t.member_sk
    ) contrib ON contrib.member_sk = m.member_sk
    LEFT JOIN (
      SELECT f.member_sk, SUM(f.amount) AS amt
        FROM WH_DW.FACT_FEE f
        JOIN WH_DW.DIM_DATE d ON d.date_key = f.date_key
       WHERE d.fy_year = EXTRACT(YEAR FROM p_run_dt)
       GROUP BY f.member_sk
    ) fee ON fee.member_sk = m.member_sk
    LEFT JOIN WH_DW.BRIDGE_MEMBER_ADVISER ma
           ON ma.member_sk = m.member_sk AND ma.current_flg = 'Y'
    LEFT JOIN WH_DW.DIM_ADVISER adv
           ON adv.adviser_sk = ma.adviser_sk
    LEFT JOIN (
      SELECT member_sk, MAX(d.full_date) AS last_dt
        FROM WH_DW.FACT_TRANSACTION t
        JOIN WH_DW.DIM_DATE d ON d.date_key = t.date_key
       GROUP BY member_sk
    ) txn ON txn.member_sk = m.member_sk
    WHERE m.current_flg = 'Y';

    COMMIT;
  END refresh_member_360;


  PROCEDURE refresh_rpt_member_dashboard IS
  BEGIN
    DELETE FROM WH_RPT.RPT_MEMBER_DASHBOARD;

    INSERT INTO WH_RPT.RPT_MEMBER_DASHBOARD (
      member_sk, given_name, surname, state_cd,
      current_account_count, total_market_value,
      ytd_contribution_amt, ytd_fee_amt, primary_adviser, last_txn_dt, refresh_dt
    )
    SELECT
      m.member_sk,
      m.given_name,
      m.surname,
      m.state_cd,
      m360.current_account_count,
      m360.total_market_value,
      m360.ytd_contribution_amt,
      m360.ytd_fee_amt,
      adv.given_name || ' ' || adv.surname AS primary_adviser,
      m360.last_txn_dt,
      SYSTIMESTAMP
    FROM WH_DW.DIM_MEMBER m
    JOIN WH_DW.INT_MEMBER_360 m360 ON m360.member_sk = m.member_sk
    LEFT JOIN WH_DW.DIM_ADVISER adv ON adv.adviser_sk = m360.primary_adviser_sk
    WHERE m.current_flg = 'Y';

    COMMIT;
  END refresh_rpt_member_dashboard;

END PKG_MEMBER_LOAD;
/
