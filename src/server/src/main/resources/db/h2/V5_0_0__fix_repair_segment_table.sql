--
--  Copyright 2017-2017 Spotify AB
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
-- Fix inconsistencies in the repair_segment table
-- to comply with new safety policies on segments
--

UPDATE repair_segment
SET end_time = NULL
WHERE end_time IS NOT NULL
AND state != 2;

UPDATE repair_segment
SET start_time = end_time
WHERE start_time is NULL
AND   end_time IS NOT NULL;


