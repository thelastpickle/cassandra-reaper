--
--  Copyright 2018-2018 Stefan Podkowinski
--  Copyright 2019-2019 The Last Pickle Ltd
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
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
