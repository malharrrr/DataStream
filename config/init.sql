
CREATE TABLE IF NOT EXISTS ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    vwap DOUBLE PRECISION,
    tick_count INTEGER,
    data_source TEXT,
    received_at TIMESTAMPTZ NOT NULL
);

-- converting into a timescaleDB hypertable
SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);

-- creating an index for fast lookup
CREATE INDEX ix_symbol_time ON ticks (symbol, time DESC);