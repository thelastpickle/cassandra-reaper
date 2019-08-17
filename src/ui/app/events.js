//
//  Copyright 2018-2018 Stefan Podkowinski
//  Copyright 2019-2019 The Last Pickle Ltd
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

import jQuery from "jquery";
import React from "react";
import ReactDOM from "react-dom";
import eventScreen from "jsx/event-screen";
import {
  clusterNames, statusObservableTimer, getClusterStatusSubject, clusterStatusResult, clusterSelected,
  logoutSubject, logoutResult, addSubscriptionSubject, addSubscriptionResult,
  eventSubscriptions, deleteSubscriptionSubject, deleteSubscriptionResult,
  listenSubscriptionSubject, unlistenSubscriptionSubject, diagnosticEvents
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


  ReactDOM.render(
    React.createElement(eventScreen, {clusterNames, currentCluster, statusObservableTimer, diagnosticEvents,
      addSubscriptionSubject, addSubscriptionResult, eventSubscriptions,
      getClusterStatusSubject, clusterStatusResult, logoutSubject, logoutResult,
      deleteSubscriptionSubject, deleteResult: deleteSubscriptionResult,
      listenSubscriptionSubject, unlistenSubscriptionSubject, clusterSelected}),
    document.getElementById('wrapper')
  );
});
