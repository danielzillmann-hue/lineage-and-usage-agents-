-- ───────────────────────────────────────────────────────────────────────────
-- Views — these give the lineage agent rich column-level edges to trace.
-- ───────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW WH_DW.V_MEMBER_PRIMARY AS
SELECT
  m.member_sk,
  m.member_natural_key,
  m.given_name,
  m.surname,
  m.dob,
  m.email,
  m.phone,
  m.state_cd,
  m.status_cd,
  m.joined_dt,
  m.src_sys
FROM WH_DW.DIM_MEMBER m
WHERE m.current_flg = 'Y';

CREATE OR REPLACE VIEW WH_DW.V_ACCOUNT_CURRENT AS
SELECT
  a.account_sk,
  a.account_natural_key,
  a.member_sk,
  a.product_sk,
  a.open_dt,
  a.close_dt,
  a.status_cd,
  a.src_sys
FROM WH_DW.DIM_ACCOUNT a
WHERE a.current_flg = 'Y';

CREATE OR REPLACE VIEW WH_DW.V_HOLDING_LATEST AS
SELECT
  h.account_sk,
  h.member_sk,
  h.fund_sk,
  h.unit_count,
  h.unit_price,
  h.market_value_amt,
  h.date_key
FROM WH_DW.FACT_HOLDING_DAILY h
WHERE h.date_key = (SELECT MAX(date_key) FROM WH_DW.DIM_DATE WHERE is_business_day = 'Y');

CREATE OR REPLACE VIEW WH_RPT.V_MEMBER_DASHBOARD AS
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
  m360.refresh_dt
FROM WH_DW.V_MEMBER_PRIMARY m
JOIN WH_DW.INT_MEMBER_360 m360 ON m360.member_sk = m.member_sk
LEFT JOIN WH_DW.DIM_ADVISER adv ON adv.adviser_sk = m360.primary_adviser_sk;

CREATE OR REPLACE VIEW WH_RPT.V_ADVISER_BOOK_OF_BUSINESS AS
SELECT
  a.adviser_sk,
  a.given_name || ' ' || a.surname AS adviser_full_name,
  a.practice_cd,
  a.state_cd,
  COUNT(DISTINCT b.member_sk) AS member_count,
  COUNT(DISTINCT acc.account_sk) AS account_count,
  SUM(m360.total_market_value) AS total_funds_under_advice,
  SUM(m360.ytd_fee_amt) AS ytd_revenue,
  CURRENT_TIMESTAMP AS refresh_dt
FROM WH_DW.DIM_ADVISER a
JOIN WH_DW.BRIDGE_MEMBER_ADVISER b ON b.adviser_sk = a.adviser_sk AND b.current_flg = 'Y'
JOIN WH_DW.INT_MEMBER_360 m360 ON m360.member_sk = b.member_sk
LEFT JOIN WH_DW.V_ACCOUNT_CURRENT acc ON acc.member_sk = b.member_sk
GROUP BY a.adviser_sk, a.given_name, a.surname, a.practice_cd, a.state_cd;

CREATE OR REPLACE VIEW WH_RPT.V_PRODUCT_HOLDINGS_SUMMARY AS
SELECT
  p.product_sk,
  hd.date_key,
  COUNT(DISTINCT a.account_sk) AS account_count,
  COUNT(DISTINCT a.member_sk) AS member_count,
  SUM(hd.market_value_amt) AS total_market_value,
  CURRENT_TIMESTAMP AS refresh_dt
FROM WH_DW.FACT_HOLDING_DAILY hd
JOIN WH_DW.DIM_ACCOUNT a ON a.account_sk = hd.account_sk
JOIN WH_DW.DIM_PRODUCT p ON p.product_sk = a.product_sk
GROUP BY p.product_sk, hd.date_key;

CREATE OR REPLACE VIEW WH_RPT.V_FUND_FLOWS_WEEKLY AS
SELECT
  ft.fund_sk,
  TRUNC(d.full_date, 'IW') AS week_start_dt,
  SUM(CASE WHEN ft.txn_type_cd IN ('CONTRIB', 'BUY') THEN ft.amount ELSE 0 END) AS inflow_amt,
  SUM(CASE WHEN ft.txn_type_cd IN ('REDEEM', 'SELL') THEN ft.amount ELSE 0 END) AS outflow_amt,
  SUM(ft.amount) AS net_flow_amt,
  CURRENT_TIMESTAMP AS refresh_dt
FROM WH_DW.FACT_TRANSACTION ft
JOIN WH_DW.DIM_DATE d ON d.date_key = ft.date_key
GROUP BY ft.fund_sk, TRUNC(d.full_date, 'IW');

-- A staging-layer consolidation view (used by integration loads)
CREATE OR REPLACE VIEW WH_STG.V_MEMBER_CONFORMED AS
SELECT 'MLC' AS src_sys, member_no AS member_natural_key,
       given_name, surname, dob, email, phone, postcode, state_cd, status_cd, joined_dt
FROM WH_RAW.RAW_MEMBER_MLC
UNION ALL
SELECT 'IOOF', client_id, first_name, last_name, date_of_birth, email_address, contact_number,
       post_code, state_code, client_status, joined_date
FROM WH_RAW.RAW_MEMBER_IOOF
UNION ALL
SELECT 'ANZW', customer_no, REGEXP_SUBSTR(given_names, '[^ ]+', 1, 1), family_name, birth_date,
       email, mobile, postcode, state_abbr, cust_status, effective_dt
FROM WH_RAW.RAW_MEMBER_ANZW
UNION ALL
SELECT 'PNDL', investor_id, REGEXP_SUBSTR(legal_name, '[^ ]+', 1, 1),
       REGEXP_SUBSTR(legal_name, '[^ ]+', 1, 2), dob, email, phone, postcode, state_cd, status, onboarded_dt
FROM WH_RAW.RAW_MEMBER_PNDL;
