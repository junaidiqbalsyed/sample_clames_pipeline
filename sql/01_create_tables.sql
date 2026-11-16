-- Source OLTP schema for the smart-claims project (Azure SQL).
-- Column names mirror the CSV headers in data/sql_server/ so lineage stays obvious.
-- Run order matters: parents first, children last. Drops go in reverse order.

IF OBJECT_ID('dbo.claims',    'U') IS NOT NULL DROP TABLE dbo.claims;
GO
IF OBJECT_ID('dbo.policies',  'U') IS NOT NULL DROP TABLE dbo.policies;
GO
IF OBJECT_ID('dbo.customers', 'U') IS NOT NULL DROP TABLE dbo.customers;
GO

CREATE TABLE dbo.customers (
    customer_id    INT           NOT NULL,
    date_of_birth  DATE          NULL,                -- 15% unknown in source
    borough        VARCHAR(20)   NULL,
    neighborhood   VARCHAR(60)   NULL,
    zip_code       CHAR(5)       NULL,                -- text, not INT: keeps leading zeros
    name           NVARCHAR(100) NOT NULL,            -- "Last, First"; NVARCHAR for non-ASCII names
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);
GO

CREATE TABLE dbo.policies (
    POLICY_NO        VARCHAR(20)   NOT NULL,          -- text: some numbers carry suffixes like /1 or /RI
    CUST_ID          INT           NOT NULL,
    POLICYTYPE       VARCHAR(10)   NOT NULL,
    POL_ISSUE_DATE   DATE          NOT NULL,
    POL_EFF_DATE     DATE          NOT NULL,
    POL_EXPIRY_DATE  DATE          NOT NULL,
    MAKE             VARCHAR(50)   NOT NULL,
    MODEL            VARCHAR(50)   NULL,
    MODEL_YEAR       SMALLINT      NULL,
    CHASSIS_NO       VARCHAR(50)   NOT NULL,          -- NOT unique: one car can have several policies
    USE_OF_VEHICLE   VARCHAR(30)   NULL,
    PRODUCT          VARCHAR(30)   NOT NULL,
    SUM_INSURED      DECIMAL(12,2) NOT NULL,
    PREMIUM          DECIMAL(12,2) NOT NULL,          -- source has refunds, so negatives are legal
    DEDUCTABLE       INT           NOT NULL,          -- (sic) spelling kept from the source file
    CONSTRAINT pk_policies PRIMARY KEY (POLICY_NO),
    CONSTRAINT fk_policies_customers FOREIGN KEY (CUST_ID) REFERENCES dbo.customers (customer_id),
    CONSTRAINT ck_policies_type  CHECK (POLICYTYPE IN ('COMP', 'TP')),
    CONSTRAINT ck_policies_dates CHECK (POL_EXPIRY_DATE >= POL_EFF_DATE)
);
GO
CREATE INDEX ix_policies_cust_id    ON dbo.policies (CUST_ID);
GO
CREATE INDEX ix_policies_chassis_no ON dbo.policies (CHASSIS_NO);   -- join key to telematics
GO

CREATE TABLE dbo.claims (
    claim_no                    UNIQUEIDENTIFIER NOT NULL,
    policy_no                   VARCHAR(20)      NOT NULL,
    claim_date                  DATE             NOT NULL,
    months_as_customer          INT              NOT NULL,
    injury                      INT              NOT NULL,
    property                    INT              NOT NULL,
    vehicle                     INT              NOT NULL,
    total                       INT              NOT NULL,
    collision_type              VARCHAR(20)      NULL,   -- unknown for e.g. parked-car claims
    number_of_vehicles_involved TINYINT          NOT NULL,
    driver_age                  SMALLINT         NOT NULL,   -- age of the driver at claim time (CSV column: age)
    insured_relationship        VARCHAR(20)      NOT NULL,
    license_issue_date          DATE             NOT NULL,
    incident_date               DATE             NOT NULL,   -- (CSV column: date)
    incident_hour               TINYINT          NOT NULL,   -- 0-23 (CSV column: hour)
    incident_type               VARCHAR(30)      NOT NULL,   -- (CSV column: type)
    incident_severity           VARCHAR(20)      NOT NULL,   -- (CSV column: severity)
    number_of_witnesses         TINYINT          NOT NULL,
    suspicious_activity         BIT              NOT NULL,
    CONSTRAINT pk_claims PRIMARY KEY (claim_no),
    CONSTRAINT fk_claims_policies FOREIGN KEY (policy_no) REFERENCES dbo.policies (POLICY_NO),
    CONSTRAINT ck_claims_total  CHECK (total = injury + property + vehicle),
    CONSTRAINT ck_claims_hour   CHECK (incident_hour BETWEEN 0 AND 23),
    CONSTRAINT ck_claims_months CHECK (months_as_customer >= 0)
);
GO
CREATE INDEX ix_claims_policy_no ON dbo.claims (policy_no);
GO
