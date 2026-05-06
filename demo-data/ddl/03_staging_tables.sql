-- ───────────────────────────────────────────────────────────────────────────
-- STAGING — cleansed and conformed across acquired platforms.
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE WH_STG.STG_MEMBER (
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
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_member PRIMARY KEY (member_natural_key, src_sys)
);

CREATE TABLE WH_STG.STG_ACCOUNT (
  account_natural_key VARCHAR2(40) NOT NULL,
  member_natural_key  VARCHAR2(40) NOT NULL,
  src_sys             VARCHAR2(8) NOT NULL,
  product_cd          VARCHAR2(16),
  open_dt             DATE,
  close_dt            DATE,
  status_cd           VARCHAR2(8),
  balance_amt         NUMBER(18,2),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_account PRIMARY KEY (account_natural_key, src_sys)
);

CREATE TABLE WH_STG.STG_TRANSACTION (
  txn_natural_key     VARCHAR2(40) NOT NULL,
  account_natural_key VARCHAR2(40) NOT NULL,
  src_sys             VARCHAR2(8) NOT NULL,
  txn_dt              DATE NOT NULL,
  txn_type_cd         VARCHAR2(8),
  amount              NUMBER(18,2),
  unit_count          NUMBER(18,4),
  unit_price          NUMBER(18,4),
  fund_code           VARCHAR2(16),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_transaction PRIMARY KEY (txn_natural_key)
);

CREATE TABLE WH_STG.STG_HOLDING (
  account_natural_key VARCHAR2(40) NOT NULL,
  fund_code           VARCHAR2(16) NOT NULL,
  asof_dt             DATE NOT NULL,
  unit_count          NUMBER(18,4),
  unit_price          NUMBER(18,4),
  market_value_amt    NUMBER(18,2),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_holding PRIMARY KEY (account_natural_key, fund_code, asof_dt)
);

CREATE TABLE WH_STG.STG_FEE (
  fee_natural_key     VARCHAR2(40) NOT NULL,
  account_natural_key VARCHAR2(40) NOT NULL,
  fee_type_cd         VARCHAR2(8),
  fee_dt              DATE,
  amount              NUMBER(18,2),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_fee PRIMARY KEY (fee_natural_key)
);

CREATE TABLE WH_STG.STG_ADVISER (
  adviser_natural_key VARCHAR2(40) NOT NULL,
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  practice_cd         VARCHAR2(16),
  state_cd            VARCHAR2(8),
  active_flg          CHAR(1),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_adviser PRIMARY KEY (adviser_natural_key, src_sys)
);

CREATE TABLE WH_STG.STG_PRODUCT (
  product_cd          VARCHAR2(16) NOT NULL,
  src_sys             VARCHAR2(8) NOT NULL,
  product_name        VARCHAR2(120),
  product_type_cd     VARCHAR2(16),
  asset_class_cd      VARCHAR2(16),
  active_flg          CHAR(1),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_product PRIMARY KEY (product_cd, src_sys)
);

CREATE TABLE WH_STG.STG_MEMBER_ADVISER_LINK (
  member_natural_key  VARCHAR2(40),
  adviser_natural_key VARCHAR2(40),
  effective_dt        DATE,
  src_sys             VARCHAR2(8),
  load_dt             TIMESTAMP
);

CREATE TABLE WH_STG.STG_FUND_PRICE (
  fund_code           VARCHAR2(16) NOT NULL,
  price_dt            DATE NOT NULL,
  unit_price          NUMBER(18,4),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_stg_fund_price PRIMARY KEY (fund_code, price_dt)
);

-- Suspicious / orphan staging — write-only or duplicated
CREATE TABLE WH_STG.STG_MEMBER_MLC_ORPHAN (
  member_no           VARCHAR2(20),
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  legacy_payload      CLOB,
  load_dt             TIMESTAMP
);

CREATE TABLE WH_STG.STG_PORTFOLIO_REBALANCE_TEMP (
  rebalance_id        VARCHAR2(40),
  account_natural_key VARCHAR2(40),
  proposed_alloc      CLOB,
  load_dt             TIMESTAMP
);

CREATE TABLE WH_STG.STG_CASE_NOTES_LEGACY (
  note_id             VARCHAR2(40),
  member_natural_key  VARCHAR2(40),
  note_text           CLOB,
  author_id           VARCHAR2(20),
  noted_dt            TIMESTAMP,
  load_dt             TIMESTAMP
);

CREATE TABLE WH_STG.STG_ADVISER_DUP_LEGACY (
  adviser_id          VARCHAR2(20),
  full_name           VARCHAR2(160),
  practice_cd         VARCHAR2(16),
  region_cd           VARCHAR2(8),
  load_dt             TIMESTAMP
);
