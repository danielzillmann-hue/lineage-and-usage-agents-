-- ───────────────────────────────────────────────────────────────────────────
-- RAW layer — verbatim extracts from acquired platforms.
-- Source codes: MLC, IOOF, ANZW, PNDL (Pendal), ASSURE (legacy admin platform).
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE WH_RAW.RAW_MEMBER_MLC (
  source_id           VARCHAR2(40) NOT NULL,
  member_no           VARCHAR2(20) NOT NULL,
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  dob                 DATE,
  email               VARCHAR2(120),
  phone               VARCHAR2(40),
  postcode            VARCHAR2(10),
  state_cd            VARCHAR2(8),
  status_cd           VARCHAR2(8),
  joined_dt           DATE,
  ext_src_sys         VARCHAR2(20),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_member_mlc PRIMARY KEY (source_id, member_no)
);

CREATE TABLE WH_RAW.RAW_MEMBER_IOOF (
  source_id           VARCHAR2(40) NOT NULL,
  client_id           VARCHAR2(20) NOT NULL,
  first_name          VARCHAR2(80),
  last_name           VARCHAR2(80),
  date_of_birth       DATE,
  email_address       VARCHAR2(120),
  contact_number      VARCHAR2(40),
  post_code           VARCHAR2(10),
  state_code          VARCHAR2(8),
  client_status       VARCHAR2(8),
  joined_date         DATE,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_member_ioof PRIMARY KEY (source_id, client_id)
);

CREATE TABLE WH_RAW.RAW_MEMBER_ANZW (
  source_id           VARCHAR2(40) NOT NULL,
  customer_no         VARCHAR2(20) NOT NULL,
  given_names         VARCHAR2(120),
  family_name         VARCHAR2(80),
  birth_date          DATE,
  email               VARCHAR2(120),
  mobile              VARCHAR2(40),
  postcode            VARCHAR2(10),
  state_abbr          VARCHAR2(8),
  cust_status         VARCHAR2(8),
  effective_dt        DATE,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_member_anzw PRIMARY KEY (source_id, customer_no)
);

CREATE TABLE WH_RAW.RAW_MEMBER_PNDL (
  source_id           VARCHAR2(40) NOT NULL,
  investor_id         VARCHAR2(20) NOT NULL,
  legal_name          VARCHAR2(160),
  dob                 DATE,
  email               VARCHAR2(120),
  phone               VARCHAR2(40),
  postcode            VARCHAR2(10),
  state_cd            VARCHAR2(8),
  status              VARCHAR2(8),
  onboarded_dt        DATE,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_member_pndl PRIMARY KEY (source_id, investor_id)
);

CREATE TABLE WH_RAW.RAW_ACCOUNT_MLC (
  source_id           VARCHAR2(40) NOT NULL,
  account_no          VARCHAR2(24) NOT NULL,
  member_no           VARCHAR2(20) NOT NULL,
  product_cd          VARCHAR2(16),
  open_dt             DATE,
  close_dt            DATE,
  status_cd           VARCHAR2(8),
  balance_amt         NUMBER(18,2),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_account_mlc PRIMARY KEY (source_id, account_no)
);

CREATE TABLE WH_RAW.RAW_ACCOUNT_IOOF (
  source_id           VARCHAR2(40) NOT NULL,
  account_id          VARCHAR2(24) NOT NULL,
  client_id           VARCHAR2(20) NOT NULL,
  product_code        VARCHAR2(16),
  opened_date         DATE,
  closed_date         DATE,
  status              VARCHAR2(8),
  current_balance     NUMBER(18,2),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_account_ioof PRIMARY KEY (source_id, account_id)
);

CREATE TABLE WH_RAW.RAW_ACCOUNT_ANZW (
  source_id           VARCHAR2(40) NOT NULL,
  account_id          VARCHAR2(24) NOT NULL,
  customer_no         VARCHAR2(20) NOT NULL,
  product_cd          VARCHAR2(16),
  open_dt             DATE,
  close_dt            DATE,
  status_cd           VARCHAR2(8),
  balance_amt         NUMBER(18,2),
  load_dt             TIMESTAMP NOT NULL
);

CREATE TABLE WH_RAW.RAW_ACCOUNT_PNDL (
  source_id           VARCHAR2(40) NOT NULL,
  account_ref         VARCHAR2(24) NOT NULL,
  investor_id         VARCHAR2(20) NOT NULL,
  product             VARCHAR2(16),
  inception_dt        DATE,
  closed_dt           DATE,
  status              VARCHAR2(8),
  market_value_amt    NUMBER(18,2),
  load_dt             TIMESTAMP NOT NULL
);

CREATE TABLE WH_RAW.RAW_TRANSACTION_FEED (
  source_id           VARCHAR2(40) NOT NULL,
  txn_id              VARCHAR2(36) NOT NULL,
  account_ref         VARCHAR2(24) NOT NULL,
  txn_dt              DATE NOT NULL,
  txn_type_cd         VARCHAR2(8),
  amount              NUMBER(18,2),
  unit_count          NUMBER(18,4),
  unit_price          NUMBER(18,4),
  fund_code           VARCHAR2(16),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_txn_feed PRIMARY KEY (source_id, txn_id)
);

CREATE TABLE WH_RAW.RAW_HOLDING_FEED (
  source_id           VARCHAR2(40) NOT NULL,
  account_ref         VARCHAR2(24) NOT NULL,
  fund_code           VARCHAR2(16) NOT NULL,
  asof_dt             DATE NOT NULL,
  unit_count          NUMBER(18,4),
  unit_price          NUMBER(18,4),
  market_value_amt    NUMBER(18,2),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_holding_feed PRIMARY KEY (account_ref, fund_code, asof_dt)
);

CREATE TABLE WH_RAW.RAW_FEE_FEED (
  source_id           VARCHAR2(40) NOT NULL,
  fee_id              VARCHAR2(36) NOT NULL,
  account_ref         VARCHAR2(24) NOT NULL,
  fee_type_cd         VARCHAR2(8),
  fee_dt              DATE,
  amount              NUMBER(18,2),
  src_sys             VARCHAR2(8) NOT NULL,
  load_dt             TIMESTAMP NOT NULL
);

CREATE TABLE WH_RAW.RAW_ADVISER_MLC (
  source_id           VARCHAR2(40) NOT NULL,
  adviser_no          VARCHAR2(20) NOT NULL,
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  practice_cd         VARCHAR2(16),
  state_cd            VARCHAR2(8),
  active_flg          CHAR(1),
  load_dt             TIMESTAMP NOT NULL
);

CREATE TABLE WH_RAW.RAW_ADVISER_IOOF (
  source_id           VARCHAR2(40) NOT NULL,
  adviser_id          VARCHAR2(20) NOT NULL,
  first_name          VARCHAR2(80),
  last_name           VARCHAR2(80),
  practice_id         VARCHAR2(16),
  state               VARCHAR2(8),
  is_active           CHAR(1),
  load_dt             TIMESTAMP NOT NULL
);

CREATE TABLE WH_RAW.RAW_PRODUCT_CATALOG (
  product_cd          VARCHAR2(16) NOT NULL,
  product_name        VARCHAR2(120) NOT NULL,
  product_type_cd     VARCHAR2(16),
  asset_class_cd      VARCHAR2(16),
  src_sys             VARCHAR2(8) NOT NULL,
  active_flg          CHAR(1),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_product_catalog PRIMARY KEY (product_cd, src_sys)
);

CREATE TABLE WH_RAW.RAW_FUND_PRICES (
  fund_code           VARCHAR2(16) NOT NULL,
  price_dt            DATE NOT NULL,
  unit_price          NUMBER(18,4),
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_fund_prices PRIMARY KEY (fund_code, price_dt)
);

CREATE TABLE WH_RAW.RAW_MEMBER_ADVISER_LINK (
  source_id           VARCHAR2(40),
  member_ref          VARCHAR2(20),
  adviser_ref         VARCHAR2(20),
  effective_dt        DATE,
  src_sys             VARCHAR2(8),
  load_dt             TIMESTAMP
);

-- Orphans / suspect ingest paths
CREATE TABLE WH_RAW.RAW_PENDAL_AUDIT_FEED (
  audit_id            VARCHAR2(40) NOT NULL,
  event_dt            TIMESTAMP NOT NULL,
  payload_clob        CLOB,
  load_dt             TIMESTAMP NOT NULL,
  CONSTRAINT pk_raw_pendal_audit_feed PRIMARY KEY (audit_id)
);

CREATE TABLE WH_RAW.RAW_TAX_FILING_TEMP (
  filing_id           VARCHAR2(40),
  member_ref          VARCHAR2(20),
  fy_year             NUMBER(4),
  payload_clob        CLOB,
  load_dt             TIMESTAMP
);

CREATE TABLE WH_RAW.RAW_CASE_NOTES_LEGACY (
  note_id             VARCHAR2(40),
  member_ref          VARCHAR2(20),
  note_text           CLOB,
  author_id           VARCHAR2(20),
  noted_dt            TIMESTAMP,
  load_dt             TIMESTAMP
);
