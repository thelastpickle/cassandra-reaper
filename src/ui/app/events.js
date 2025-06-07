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
import Cookies from "js-cookie";
import ReactDOM from "react-dom";
import eventScreen from "jsx/event-screen";
import {
  clusterNames, statusObservableTimer, getClusterStatusSubject, clusterStatusResult, clusterSelected,
  logoutSubject, logoutResult, addSubscriptionSubject, addSubscriptionResult,
  eventSubscriptions, deleteSubscriptionSubject, deleteSubscriptionResult,
  listenSubscriptionSubject, unlistenSubscriptionSubject, diagnosticEvents
} from "observable";

jQuery(document).ready(function($){
  document.documentElement.setAttribute('data-theme', Cookies.get('reaper-theme'));
  
  // Set up JWT authorization header if token exists
  const token = sessionStorage.getItem('jwtToken');
  if (token) {
    // Set cookie for servlet filter access (if not already set)
    if (!Cookies.get('jwtToken')) {
      Cookies.set('jwtToken', token, { 
        expires: 1, // 1 day default
        secure: window.location.protocol === 'https:', 
        sameSite: 'Lax' 
      });
    }
    
    $.ajaxSetup({
      beforeSend: function(xhr) {
        xhr.setRequestHeader('Authorization', 'Bearer ' + token);
      }
    });
  }
  
  $.urlParam = function(name){
    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
    if (results) {
      return results[1] || 0;
    }
    else {
      return null;
    }
  }

  let currentCluster = $.urlParam('currentCluster');
  if (!currentCluster || currentCluster === "null") {
    currentCluster = 'all';
  }

  ReactDOM.render(
    React.createElement(eventScreen, {clusterNames, currentCluster, statusObservableTimer, diagnosticEvents,
      addSubscriptionSubject, addSubscriptionResult, eventSubscriptions,
      getClusterStatusSubject, clusterStatusResult, logoutSubject, logoutResult,
      deleteSubscriptionSubject, deleteResult: deleteSubscriptionResult,
      listenSubscriptionSubject, unlistenSubscriptionSubject, clusterSelected,
      switchTheme: function(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        Cookies.set('reaper-theme', theme);
      }}),
    document.getElementById('wrapper')
  );
});
