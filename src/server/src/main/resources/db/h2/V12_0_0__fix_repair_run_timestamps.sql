--
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
-- Fix timestamp inconsistencies in the repair_run table
--

UPDATE repair_run
SET start_time = NULL
WHERE start_time is NOT NULL
AND state =  'NOT_STARTED';

UPDATE repair_run
SET end_time = NULL
WHERE end_time IS NOT NULL
AND state NOT IN ('ERROR', 'DONE', 'ABORTED', 'PAUSED');

UPDATE repair_run
SET end_time = start_time
WHERE end_time IS NULL
AND state IN ('ERROR', 'DONE', 'ABORTED', 'PAUSED');

UPDATE repair_run
SET start_time = end_time
WHERE start_time is NULL
AND end_time IS NOT NULL;

UPDATE repair_run
SET pause_time = start_time
WHERE pause_time IS NULL
AND state = 'PAUSED';
