# Copyright 2018-2019 The Last Pickle Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Feature: Access Control

  Background:
    Given cluster seed host "127.0.0.1" points to cluster with name "test"

  Scenario Outline: Request to protected resource is redirected to login page when accessed without login
    When a <path> <request> is made
    Then the response was redirected to the login page
    Examples:
      | path   | request              |
      | GET    | /webui               |
      | GET    | /webui/index.html    |

  Scenario Outline: Request to public resource is allowed without login
    When a <path> <request> is made
    Then a "OK" response is returned
    Examples:
      | path   | request              |
      | GET    | /webui/login.html    |

  Scenario Outline: Request to ping resource is allowed but not healthy
    When a <path> <request> is made
    Then a "NOT_ACCEPTABLE" response is returned
    Examples:
      | path   | request              |
      | GET    | /ping                |

  Scenario Outline: Request to protected resource without login returns forbidden
    When a <path> <request> is made
    Then a "FORBIDDEN" response is returned
    Examples:
      | path   | request              |
      | GET    | /cluster             |
      | GET    | /repair_run          |
      | GET    | /repair_schedule     |
