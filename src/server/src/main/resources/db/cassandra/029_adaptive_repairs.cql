--
--  Copyright 2021-2021 Datastax inc.
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
-- Store the repair segment timeout in the repair unit

ALTER TABLE repair_unit_v1 ADD timeout int;
ALTER TABLE repair_schedule_v1 ADD adaptive boolean;
ALTER TABLE repair_run ADD adaptive_schedule boolean static;