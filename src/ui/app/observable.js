//
//  Copyright 2015-2016 Stefan Podkowinski
//  Copyright 2016-2019 The Last Pickle Ltd
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

import Rx from "rxjs";
import $ from "jquery";
import Cookies from "js-cookie";
import { getUrlPrefix } from "jsx/mixin";

// interval to use for polling entity lists
const POLLING_INTERVAL = 2000;

// use reaper server url for ajax calls if running on dev server (will be run in iframe)
const URL_PREFIX = getUrlPrefix(window.top.location.pathname);

export const statusObservableTimer = Rx.Observable.timer(0, POLLING_INTERVAL).map(t => {
  console.debug("Pinging reaper server..");
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/ping`
  }).promise());
});

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Login
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

export const loginSubject = new Rx.Subject();

export const loginResult = loginSubject.map(login => {
  console.info("Logging in with username: " + login.username);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/login`,
    method: 'POST',
    data: { username: login.username, password: login.password, rememberMe: login.rememberMe}
  }).promise()).map(response => {
    // Store JWT token for future requests
    if (response.token) {
      sessionStorage.setItem('jwtToken', response.token);
      sessionStorage.setItem('username', response.username);
      sessionStorage.setItem('roles', JSON.stringify(response.roles));
      
      // Also store token in cookie for servlet filter access
      Cookies.set('jwtToken', response.token, { 
        expires: login.rememberMe ? 30 : 1, // 30 days if remember me, 1 day otherwise
        secure: window.location.protocol === 'https:', 
        sameSite: 'Lax' 
      });
      
      // Set default authorization header for future requests
      $.ajaxSetup({
        beforeSend: function(xhr) {
          const token = sessionStorage.getItem('jwtToken');
          if (token) {
            xhr.setRequestHeader('Authorization', 'Bearer ' + token);
          }
        }
      });
    }
    return response;
  });
}).share();

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Logout
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

export const logoutSubject = new Rx.Subject();

export const logoutResult = logoutSubject.map(logout => {
  console.info("Logging out");
  
  // Clear stored JWT token and user data
  sessionStorage.removeItem('jwtToken');
  sessionStorage.removeItem('username');
  sessionStorage.removeItem('roles');
  
  // Clear JWT cookie
  Cookies.remove('jwtToken');
  
  // Clear authorization headers
  $.ajaxSetup({
    beforeSend: function(xhr) {
      // Remove authorization header
    }
  });
  
  // Redirect to login page
  window.location.href = '/webui/login.html';
  
  // Return empty observable since we're redirecting
  return Rx.Observable.just({});
}).share();

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// common shared observables
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

export const clusterFilterSelection = new Rx.Subject();


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// cluster
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


export const addClusterSubject = new Rx.Subject();
export const deleteClusterSubject = new Rx.Subject();
export const getClusterStatusSubject = new Rx.Subject();
export const selectClusterSubject = new Rx.Subject();


export const addClusterResult = addClusterSubject.map(addCluster => {
  console.info("Adding new cluster with seed node: " + addCluster.seed_node);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/cluster/auth`,
    method: 'POST',
    data: { seedHost: addCluster.seed_node,
            jmxPort: addCluster.jmx_port,
            jmxUsername: addCluster.jmx_username,
            jmxPassword: addCluster.jmx_password}
  }).promise());
}).share();

export const deleteClusterResult = deleteClusterSubject.map(name => {
  console.info("Deleting cluster with name: " + name);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/cluster/${encodeURIComponent(name)}`,
    method: 'DELETE'
  }).promise());
}).share();


export const clusterNames = Rx.Observable.merge(
    Rx.Observable.timer(0, POLLING_INTERVAL).map(t => Rx.Observable.just({})),
    addClusterResult,
    deleteClusterResult
  ).map(s =>
    s.flatMap(t => Rx.Observable.fromPromise($.ajax({
        url: `${URL_PREFIX}/cluster`
      }).promise())
    )
);

export const clusterSelected = selectClusterSubject.share();

export const clusterStatusResult = Rx.Observable.merge(
    clusterSelected,
    getClusterStatusSubject
  ).map(clusterName => {
    console.info("Getting cluster status: " + clusterName);
    return Rx.Observable.fromPromise($.ajax({
      url: `${URL_PREFIX}/cluster/${encodeURIComponent(clusterName)}`,
      method: 'GET'
    }).promise());
  }
).share();


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// repair_run
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


export const addRepairSubject = new Rx.Subject();
export const deleteRepairSubject = new Rx.Subject();
export const updateRepairStatusSubject = new Rx.Subject();
export const updateRepairIntensitySubject = new Rx.Subject();
export const repairRunSubject = new Rx.Subject();

// The addReaperSubject handles both on demand and scheduled repairs. This is
// because the form component to fill out the repair details is used to capture
// the information for both types of repair.
export const addRepairResult = addRepairSubject.map(repair => {
  const repairParams = $.param(repair.params);
  let repairEndpoint = "repair_run";
  let messagePrefix = "Starting repair";

  if (repair.type === "schedule") {
    repairEndpoint = "repair_schedule";
    messagePrefix = "Adding new schedule";
  }

  console.info(`${messagePrefix} for cluster: ${repair.params.clusterName}`);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/${repairEndpoint}?${repairParams}`,
    method: 'POST'
  }).promise());
}).share();

export const deleteRepairResult = deleteRepairSubject.map(repair => {
  console.info("Deleting repair run with id: " + repair.id);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_run/${encodeURIComponent(repair.id)}?owner=${encodeURIComponent(repair.owner)}`,
    method: 'DELETE'
  }).promise());
}).share();

export const updateRepairStatusResult = updateRepairStatusSubject.map(repair => {
  console.info(`Updating repair run ${repair.id} status with state ${repair.state}`);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_run/${encodeURIComponent(repair.id)}/state/${encodeURIComponent(repair.state)}`,
    method: 'PUT'
  }).promise());
}).share();

export const updateRepairIntensityResult = updateRepairIntensitySubject.map(repair => {
  console.info(`Updating repair run ${repair.id} status with intensity ${repair.intensity}`);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_run/${encodeURIComponent(repair.id)}/intensity/${encodeURIComponent(repair.intensity)}`,
    method: 'PUT'
  }).promise());
}).share();

export const repairRunResult = repairRunSubject.map(repair => {
  const params = $.param(repair);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_run?${params}`,
    method: 'GET'
  }).promise());
}).share();

export const repairs = Rx.Observable.merge(
    repairRunResult,
    addRepairResult,
    deleteRepairResult,
    updateRepairStatusResult,
    updateRepairIntensityResult
  ).map(s =>
    s.flatMap(t => Rx.Observable.fromPromise($.ajax({
        url: `${URL_PREFIX}/repair_run`
      }).promise())
    ).map(arr=>arr.reverse())
);


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// repair_schedule
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

// Schedules only have delete and update subjects because the addReaperSubject
// handles creating both on demand and scheduled repairs.
// See "addRepairResult" above.
export const deleteScheduleSubject = new Rx.Subject();
export const updateScheduleStatusSubject = new Rx.Subject();

export const deleteScheduleResult = deleteScheduleSubject.map(schedule => {
  console.info("Deleting schedule with id: " + schedule.id);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_schedule/${encodeURIComponent(schedule.id)}?owner=${encodeURIComponent(schedule.owner)}`,
    method: 'DELETE'
  }).promise());
}).share();

export const updateScheduleStatusResult = updateScheduleStatusSubject.map(schedule => {
  console.info(`Updating schedule ${schedule.id} status with state ${schedule.state} `);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_schedule/${encodeURIComponent(schedule.id)}?state=${encodeURIComponent(schedule.state)}`,
    method: 'PUT'
  }).promise());
}).share();


export const schedules = Rx.Observable.merge(
    Rx.Observable.timer(0, POLLING_INTERVAL).map(t => Rx.Observable.just({})),
    addRepairResult,
    deleteScheduleResult,
    updateScheduleStatusResult
  ).map(s =>
    s.flatMap(t => Rx.Observable.fromPromise($.ajax({
        url: `${URL_PREFIX}/repair_schedule`
      }).promise())
    )
);


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// diagnostic events
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


export const addSubscriptionSubject = new Rx.Subject();
export const deleteSubscriptionSubject = new Rx.Subject();
export const listenSubscriptionSubject = new Rx.Subject();
export const unlistenSubscriptionSubject = new Rx.Subject();
export const diagnosticEvents = new Rx.Subject();

export const addSubscriptionResult = addSubscriptionSubject.map(sub => {
  console.info("Submitting events subscription: " + sub);
  const params = $.param(sub);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/diag_event/subscription?${params}`,
    method: 'POST'
  }).promise());
}).share();

export const deleteSubscriptionResult = deleteSubscriptionSubject.map(sub => {
  console.info("Deleting events subscription: " + sub.id);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/diag_event/subscription/${encodeURIComponent(sub.id)}`,
    method: 'DELETE'
  }).promise());
}).share();

export const eventSubscriptions = Rx.Observable.merge(
    clusterSelected.map(clusterName => Rx.Observable.just(clusterName)),
    addSubscriptionResult,
    deleteSubscriptionResult
  ).map(s =>
    s.flatMap(r => {
      var clusterName = r;
      if(r.cluster) {
        clusterName = r.cluster;
      }
      console.info("Getting event subscriptions: " + clusterName);
      return Rx.Observable.fromPromise($.ajax({
       url: `${URL_PREFIX}/diag_event/subscription?clusterName=${encodeURIComponent(clusterName)}`,
       method: 'GET'
      }).promise());
    })
  ).share();
