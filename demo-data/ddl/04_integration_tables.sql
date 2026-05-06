-- ───────────────────────────────────────────────────────────────────────────
-- INTEGRATION — dimensional model, post-conformance.
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE WH_DW.DIM_DATE (
  date_key            NUMBER(8) NOT NULL,
  full_date           DATE NOT NULL,
  fy_year             NUMBER(4),
  fy_quarter          NUMBER(1),
  fy_month            NUMBER(2),
  is_business_day     CHAR(1),
  CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE TABLE WH_DW.DIM_MEMBER (
  member_sk           NUMBER(18) NOT NULL,
  member_natural_key  VARCHAR2(40) NOT NULL,
  src_sys             VARCHAR2(8) NOT NULL,
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  dob                 DATE,
  email               VARCHAR2(120),
  phone               VARCHAR2(40),
  postcode            VARCHAR2(10),
  state_cd            VARCHAR2(8),
  status_cd           VARCHAR2(8),
  joined_dt           DATE,
  effective_dt        DATE NOT NULL,
  end_dt              DATE,
  current_flg         CHAR(1) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_member PRIMARY KEY (member_sk)
);

CREATE TABLE WH_DW.DIM_ACCOUNT (
  account_sk          NUMBER(18) NOT NULL,
  account_natural_key VARCHAR2(40) NOT NULL,
  member_sk           NUMBER(18),
  product_sk          NUMBER(18),
  src_sys             VARCHAR2(8) NOT NULL,
  open_dt             DATE,
  close_dt            DATE,
  status_cd           VARCHAR2(8),
  current_flg         CHAR(1) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_account PRIMARY KEY (account_sk)
);

CREATE TABLE WH_DW.DIM_PRODUCT (
  product_sk          NUMBER(18) NOT NULL,
  product_cd          VARCHAR2(16) NOT NULL,
  src_sys             VARCHAR2(8) NOT NULL,
  product_name        VARCHAR2(120),
  product_type_cd     VARCHAR2(16),
  asset_class_cd      VARCHAR2(16),
  active_flg          CHAR(1),
  CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
);

CREATE TABLE WH_DW.DIM_ADVISER (
  adviser_sk          NUMBER(18) NOT NULL,
  adviser_natural_key VARCHAR2(40) NOT NULL,
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  practice_cd         VARCHAR2(16),
  state_cd            VARCHAR2(8),
  active_flg          CHAR(1),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_dim_adviser PRIMARY KEY (adviser_sk)
);

CREATE TABLE WH_DW.DIM_FUND (
  fund_sk             NUMBER(18) NOT NULL,
  fund_code           VARCHAR2(16) NOT NULL,
  fund_name           VARCHAR2(120),
  asset_class_cd      VARCHAR2(16),
  CONSTRAINT pk_dim_fund PRIMARY KEY (fund_sk)
);

CREATE TABLE WH_DW.FACT_TRANSACTION (
  txn_sk              NUMBER(18) NOT NULL,
  txn_natural_key     VARCHAR2(40) NOT NULL,
  account_sk          NUMBER(18),
  member_sk           NUMBER(18),
  product_sk          NUMBER(18),
  fund_sk             NUMBER(18),
  date_key            NUMBER(8) NOT NULL,
  txn_type_cd         VARCHAR2(8),
  amount              NUMBER(18,2),
  unit_count          NUMBER(18,4),
  unit_price          NUMBER(18,4),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_fact_transaction PRIMARY KEY (txn_sk)
);

CREATE TABLE WH_DW.FACT_HOLDING_DAILY (
  holding_sk          NUMBER(18) NOT NULL,
  account_sk          NUMBER(18) NOT NULL,
  member_sk           NUMBER(18),
  fund_sk             NUMBER(18) NOT NULL,
  date_key            NUMBER(8) NOT NULL,
  unit_count          NUMBER(18,4),
  unit_price          NUMBER(18,4),
  market_value_amt    NUMBER(18,2),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_fact_holding_daily PRIMARY KEY (holding_sk)
);

CREATE TABLE WH_DW.FACT_FEE (
  fee_sk              NUMBER(18) NOT NULL,
  account_sk          NUMBER(18),
  member_sk           NUMBER(18),
  fee_type_cd         VARCHAR2(8),
  date_key            NUMBER(8),
  amount              NUMBER(18,2),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_fact_fee PRIMARY KEY (fee_sk)
);

CREATE TABLE WH_DW.FACT_ACCOUNT_DAILY_BALANCE (
  account_sk          NUMBER(18) NOT NULL,
  member_sk           NUMBER(18),
  date_key            NUMBER(8) NOT NULL,
  market_value_amt    NUMBER(18,2),
  units_total         NUMBER(18,4),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_fact_acct_daily PRIMARY KEY (account_sk, date_key)
);

CREATE TABLE WH_DW.BRIDGE_MEMBER_ADVISER (
  member_sk           NUMBER(18) NOT NULL,
  adviser_sk          NUMBER(18) NOT NULL,
  effective_dt        DATE NOT NULL,
  end_dt              DATE,
  current_flg         CHAR(1) NOT NULL,
  CONSTRAINT pk_bridge_mem_adv PRIMARY KEY (member_sk, adviser_sk, effective_dt)
);

CREATE TABLE WH_DW.INT_MEMBER_360 (
  member_sk           NUMBER(18) NOT NULL,
  current_account_count   NUMBER(8),
  total_market_value      NUMBER(18,2),
  ytd_contribution_amt    NUMBER(18,2),
  ytd_fee_amt             NUMBER(18,2),
  primary_adviser_sk      NUMBER(18),
  last_txn_dt             DATE,
  refresh_dt              TIMESTAMP NOT NULL,
  CONSTRAINT pk_int_member_360 PRIMARY KEY (member_sk)
);
