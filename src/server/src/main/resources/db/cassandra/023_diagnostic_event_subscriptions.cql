--
--  Copyright 2018-2019 The Last Pickle Ltd
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
-- Upgrade to handle diagnostic event subscriptions

CREATE TABLE IF NOT EXISTS diagnostic_event_subscription (
  id                      uuid PRIMARY KEY,
  cluster                 text,
  description             text,
  nodes                   set<text>,
  events                  set<text>,
  export_sse              boolean,
  export_file_logger      text,
  export_http_endpoint    text,
)
WITH compaction = {'class':'LeveledCompactionStrategy'}
   AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
   AND gc_grace_seconds = 864000
   AND default_time_to_live = 0;
