-- Event log + dedup record in one table: the primary key on event_id is the idempotency arbiter.
-- INSERT ... ON CONFLICT (event_id) DO NOTHING inside the processing txn: 0 rows = duplicate delivery.
CREATE TABLE callback_event (
    event_id        varchar(160) PRIMARY KEY,
    batch_id        varchar(64)  NOT NULL,
    event_type      varchar(80)  NOT NULL,
    subscriber_key  varchar(512) NOT NULL,
    message_key     varchar(512),
    upstream_ts     timestamptz,
    ordering_ts     timestamptz  NOT NULL,
    clock_skewed    boolean      NOT NULL DEFAULT false,
    received_at     timestamptz  NOT NULL,
    processed_at    timestamptz  NOT NULL DEFAULT now(),
    trace_id        varchar(64),
    source          varchar(16)  NOT NULL,          -- 'kafka' | 'sync'
    kafka_partition int,
    kafka_offset    bigint,
    payload         jsonb        NOT NULL
);
CREATE INDEX callback_event_message_key ON callback_event (message_key);

-- Projection: current delivery status per transactional message. Updated only when the incoming event is
-- strictly greater in (status_rank, ordering_ts, last_event_id): a max over a total order, so the final
-- state is independent of arrival order (late 'sent' never overwrites 'bounced'/'delivered').
CREATE TABLE message_status (
    message_key     varchar(512) PRIMARY KEY,
    subscriber_key  varchar(512) NOT NULL,
    channel         varchar(8)   NOT NULL,
    status          varchar(16)  NOT NULL,
    status_rank     smallint     NOT NULL,
    ordering_ts     timestamptz  NOT NULL,
    last_event_id   varchar(160) NOT NULL,
    transitions     int          NOT NULL DEFAULT 1,
    updated_at      timestamptz  NOT NULL DEFAULT now()
);

-- Sync-baseline mode only (async mode routes these to the invalid-events topic).
CREATE TABLE invalid_event (
    id          bigserial PRIMARY KEY,
    batch_id    varchar(64)  NOT NULL,
    reason      varchar(40)  NOT NULL,
    received_at timestamptz  NOT NULL,
    payload     jsonb
);

-- Experiment instrumentation: how many times the (simulated) downstream side effect ran per event.
-- calls > 1 means an at-least-once side effect was repeated (e.g. crash after the call, before commit).
CREATE TABLE downstream_call (
    event_id  varchar(160) PRIMARY KEY,
    calls     int          NOT NULL,
    first_at  timestamptz  NOT NULL DEFAULT now(),
    last_at   timestamptz  NOT NULL DEFAULT now()
);
