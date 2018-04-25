import Rx from "rxjs";
import $ from "jquery";


// interval to use for polling entity lists
const POLLING_INTERVAL = 2000;

// use reaper server url for ajax calls if running on dev server (will be run in iframe)
const isDev = window.top.location.pathname.includes('webpack-dev-server');
const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';

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
    data: { username: login.username, password: login.password}
  }).promise());
}).share();

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Logout
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

export const logoutSubject = new Rx.Subject();

export const logoutResult = logoutSubject.map(logout => {
  console.info("Logging out");
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/logout`,
    method: 'POST'
  }).promise());
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


export const addClusterResult = addClusterSubject.map(seed => {
  console.info("Adding new cluster with seed node: " + seed);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/cluster?seedHost=${encodeURIComponent(seed)}`,
    method: 'POST'
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
// repair_schedule
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


export const addScheduleSubject = new Rx.Subject();
export const deleteScheduleSubject = new Rx.Subject();
export const updateScheduleStatusSubject = new Rx.Subject();


export const addScheduleResult = addScheduleSubject.map(schedule => {
  console.info("Adding new schedule for cluster: " + schedule.clusterName);
  const params = $.param(schedule);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_schedule?${params}`,
    method: 'POST'
  }).promise());
}).share();

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
    addScheduleResult,
    deleteScheduleResult,
    updateScheduleStatusResult
  ).map(s =>
    s.flatMap(t => Rx.Observable.fromPromise($.ajax({
        url: `${URL_PREFIX}/repair_schedule`
      }).promise())
    )
);


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// repair_run
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


export const addRepairSubject = new Rx.Subject();
export const deleteRepairSubject = new Rx.Subject();
export const updateRepairStatusSubject = new Rx.Subject();
export const updateRepairIntensitySubject = new Rx.Subject();


export const addRepairResult = addRepairSubject.map(repair => {
  console.info("Starting repair for cluster: " + repair.clusterName);
  const params = $.param(repair);
  return Rx.Observable.fromPromise($.ajax({
    url: `${URL_PREFIX}/repair_run?${params}`,
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


export const repairs = Rx.Observable.merge(
    Rx.Observable.timer(0, POLLING_INTERVAL).map(t => Rx.Observable.just({})),
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
// diagnostic events
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


export const addSubscriptionSubject = new Rx.Subject();
export const deleteSubscriptionSubject = new Rx.Subject();
export const listenSubscriptionSubject = new Rx.Subject();
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