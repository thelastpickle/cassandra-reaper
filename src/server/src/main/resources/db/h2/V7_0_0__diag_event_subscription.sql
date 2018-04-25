--
-- Diagnostic event subscriptions
--

CREATE TABLE IF NOT EXISTS diag_event_subscription (
  id                      SERIAL PRIMARY KEY,
  cluster                 VARCHAR NOT NULL,
  description             VARCHAR,
  include_nodes           ARRAY NOT NULL,
  events                  ARRAY NOT NULL,
  export_sse              BOOLEAN NOT NULL DEFAULT FALSE,
  export_file_logger      VARCHAR NULL,
  export_http_endpoint    VARCHAR NULL
);