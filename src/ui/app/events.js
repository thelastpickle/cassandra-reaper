import jQuery from "jquery";
import React from "react";
import ReactDOM from "react-dom";
import eventScreen from "jsx/event-screen";
import {
  clusterNames, statusObservableTimer, getClusterStatusSubject, clusterStatusResult,
  logoutSubject, logoutResult, addSubscriptionSubject, addSubscriptionResult,
  eventSubscriptions, deleteSubscriptionSubject, deleteSubscriptionResult, listenSubscriptionSubject, diagnosticEvents
} from "observable";

jQuery(document).ready(function($){

  $.urlParam = function(name){
    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
    if (results != null) {
      return results[1] || 0;
    }
    else {
      return null;
    }
  }

  let currentCluster: string = $.urlParam('currentCluster');
  if(!currentCluster) {
    currentCluster = 'all';
  }


  const isDev = window != window.top;
  const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';


//  const subscriptionsSubject = new Rx.Subject();
//
//  const logoutResult = subscriptionsSubject.map(logout => {
//    console.info("Logging out");
//    return Rx.Observable.fromPromise($.ajax({
//      url: `${URL_PREFIX}/logout`,
//      method: 'POST'
//    }).promise());
//  }).share();



//  const statusObservableTimer = Rx.Observable.timer(0, POLLING_INTERVAL).map(t => {
//    console.debug("Pinging reaper server..");
//    return Rx.Observable.fromPromise($.ajax({
//      url: `${URL_PREFIX}/ping`
//    }).promise());
//  });

  listenSubscriptionSubject.subscribeOnNext(id => {

    const source = new EventSource(`${URL_PREFIX}/diag_event/sse_listen/${id}`);
    source.onmessage = function(event) {
      const obj = $.parseJSON(event.data);
      console.debug(`Received diagnostic event (${event.lastEventId})`, obj);
      diagnosticEvents.onNext({eventData: obj, eventId: event.lastEventId});
    };
    source.onerror = function(err) {
      diagnosticEvents.onError(err);
    };
    source.onopen = function() {
      console.debug("EventSource ready for receiving diagnostic events");
    };

  });


  ReactDOM.render(
    React.createElement(eventScreen, {clusterNames, currentCluster, statusObservableTimer, diagnosticEvents,
      addSubscriptionSubject, addSubscriptionResult, eventSubscriptions,
      getClusterStatusSubject, clusterStatusResult, logoutSubject, logoutResult,
      deleteSubscriptionSubject, deleteResult: deleteSubscriptionResult,
      listenSubscriptionSubject: listenSubscriptionSubject}),
    document.getElementById('wrapper')
  );
});
