-- ───────────────────────────────────────────────────────────────────────────
-- WH_LEGACY — leftovers from acquisitions. Should surface as dead/orphan.
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE WH_LEGACY.LEG_MLC_MEMBER_ARCHIVE (
  member_no           VARCHAR2(20),
  given_name          VARCHAR2(80),
  surname             VARCHAR2(80),
  dob                 DATE,
  archived_dt         TIMESTAMP
);

CREATE TABLE WH_LEGACY.LEG_IOOF_FUND_ARCHIVE (
  fund_code           VARCHAR2(16),
  fund_name           VARCHAR2(120),
  closed_dt           DATE
);

CREATE TABLE WH_LEGACY.LEG_PORTFOLIO_REBALANCE_QUEUE (
  rebalance_id        VARCHAR2(40),
  account_ref         VARCHAR2(24),
  proposed_alloc      CLOB,
  queued_dt           TIMESTAMP
);

CREATE TABLE WH_LEGACY.LEG_ANZ_CLIENT_OVERRIDES (
  customer_no         VARCHAR2(20),
  override_field      VARCHAR2(40),
  override_value      VARCHAR2(200),
  applied_dt          DATE
);

CREATE OR REPLACE VIEW WH_LEGACY.V_MLC_MEMBER_PHONE_AUDIT AS
SELECT m.member_no, m.given_name, m.surname, r.phone
FROM WH_LEGACY.LEG_MLC_MEMBER_ARCHIVE m
JOIN WH_RAW.RAW_MEMBER_MLC r ON r.member_no = m.member_no;
