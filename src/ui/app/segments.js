// Copyright 2016-2018 The Last Pickle Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import jQuery from "jquery";
import ReactDOM from "react-dom";
import React from "react";
import Cookies from "js-cookie";
import SegmentList from "jsx/segment-list";
import {
  statusObservableTimer,
  addClusterSubject, addClusterResult, deleteClusterSubject, deleteClusterResult,
  clusterNames,
  logoutSubject, logoutResult
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


  let repairRunId = $.urlParam('repairRunId');

  if (!repairRunId) {
    repairRunId = '0';
  }

  ReactDOM.render(
    React.createElement(SegmentList, {repairRunId, logoutSubject: logoutSubject, logoutResult: logoutResult}),
    document.getElementById('wrapper')
  );
});
