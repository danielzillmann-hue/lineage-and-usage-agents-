-- ───────────────────────────────────────────────────────────────────────────
-- REPORTING — aggregates feeding Power BI / Cognos.
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE WH_RPT.RPT_MEMBER_DASHBOARD (
  member_sk           NUMBER(18) NOT NULL,
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  state_cd            VARCHAR2(8),
  current_account_count NUMBER(8),
  total_market_value    NUMBER(18,2),
  ytd_contribution_amt  NUMBER(18,2),
  ytd_fee_amt           NUMBER(18,2),
  primary_adviser       VARCHAR2(160),
  last_txn_dt           DATE,
  refresh_dt            TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_member_dashboard PRIMARY KEY (member_sk)
);

CREATE TABLE WH_RPT.RPT_ADVISER_BOOK_OF_BUSINESS (
  adviser_sk          NUMBER(18) NOT NULL,
  adviser_full_name   VARCHAR2(160),
  practice_cd         VARCHAR2(16),
  state_cd            VARCHAR2(8),
  member_count        NUMBER(8),
  account_count       NUMBER(8),
  total_funds_under_advice NUMBER(18,2),
  ytd_revenue         NUMBER(18,2),
  refresh_dt          TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_adviser_bob PRIMARY KEY (adviser_sk)
);

CREATE TABLE WH_RPT.RPT_FEE_LEAKAGE (
  account_sk          NUMBER(18) NOT NULL,
  member_sk           NUMBER(18),
  fee_type_cd         VARCHAR2(8),
  expected_fee_amt    NUMBER(18,2),
  actual_fee_amt      NUMBER(18,2),
  variance_amt        NUMBER(18,2),
  fy_period           NUMBER(6),
  refresh_dt          TIMESTAMP NOT NULL
);

CREATE TABLE WH_RPT.RPT_PORTFOLIO_PERFORMANCE_DAILY (
  account_sk          NUMBER(18) NOT NULL,
  date_key            NUMBER(8) NOT NULL,
  market_value_amt    NUMBER(18,2),
  daily_return_pct    NUMBER(8,5),
  ytd_return_pct      NUMBER(8,5),
  refresh_dt          TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_portfolio_perf PRIMARY KEY (account_sk, date_key)
);

CREATE TABLE WH_RPT.RPT_FUND_FLOWS_WEEKLY (
  fund_sk             NUMBER(18) NOT NULL,
  week_start_dt       DATE NOT NULL,
  inflow_amt          NUMBER(18,2),
  outflow_amt         NUMBER(18,2),
  net_flow_amt        NUMBER(18,2),
  refresh_dt          TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_fund_flows PRIMARY KEY (fund_sk, week_start_dt)
);

CREATE TABLE WH_RPT.RPT_REGULATORY_SUPER_RETURN (
  fy_year             NUMBER(4) NOT NULL,
  fund_sk             NUMBER(18) NOT NULL,
  member_count        NUMBER(10),
  total_assets_amt    NUMBER(20,2),
  total_contributions_amt NUMBER(20,2),
  total_benefits_amt  NUMBER(20,2),
  refresh_dt          TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_super_return PRIMARY KEY (fy_year, fund_sk)
);

CREATE TABLE WH_RPT.RPT_CHURN_ANALYSIS (
  member_sk           NUMBER(18) NOT NULL,
  churn_risk_score    NUMBER(5,2),
  primary_driver      VARCHAR2(40),
  refresh_dt          TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_churn PRIMARY KEY (member_sk)
);

CREATE TABLE WH_RPT.RPT_PRODUCT_HOLDINGS_SUMMARY (
  product_sk          NUMBER(18) NOT NULL,
  date_key            NUMBER(8) NOT NULL,
  account_count       NUMBER(10),
  member_count        NUMBER(10),
  total_market_value  NUMBER(20,2),
  refresh_dt          TIMESTAMP NOT NULL,
  CONSTRAINT pk_rpt_product_summary PRIMARY KEY (product_sk, date_key)
);
