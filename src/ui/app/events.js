import jQuery from "jquery";
import React from "react";
import ReactDOM from "react-dom";
import eventScreen from "jsx/event-screen";
import {
  clusterNames, statusObservableTimer, getClusterStatusSubject, clusterStatusResult
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

  const diagnosticEvents = new Rx.Subject();
  const isDev = window != window.top;
  const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';

  const source = new EventSource(`${URL_PREFIX}/events/listen`);
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

  ReactDOM.render(
    React.createElement(eventScreen, {clusterNames, currentCluster, statusObservableTimer, diagnosticEvents,
      getClusterStatusSubject, clusterStatusResult}),
    document.getElementById('wrapper')
  );
});
