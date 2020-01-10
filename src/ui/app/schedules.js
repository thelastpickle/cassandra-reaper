//
//  Copyright 2015-2016 Stefan Podkowinski
//  Copyright 2016-2018 The Last Pickle Ltd
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
import Cookies from "js-cookie";
import ScheduleScreen from "jsx/schedule-screen";
import {
  statusObservableTimer,
  addRepairSubject, addRepairResult, deleteScheduleSubject, deleteScheduleResult,
  updateScheduleStatusSubject, updateScheduleStatusResult,
  schedules, clusterNames,
  logoutSubject, logoutResult
} from "observable";

jQuery(document).ready(function($){
  document.documentElement.setAttribute('data-theme', Cookies.get('reaper-theme'));
  $.urlParam = function(name){
    let results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
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
    React.createElement(ScheduleScreen, {clusterNames, addRepairSubject, addRepairResult, currentCluster, schedules,
      logoutSubject: logoutSubject, logoutResult: logoutResult, 
      deleteSubject: deleteScheduleSubject,
    deleteResult: deleteScheduleResult, updateStatusSubject: updateScheduleStatusSubject, statusObservableTimer,
    switchTheme: function(theme) {
      document.documentElement.setAttribute('data-theme', theme);
      Cookies.set('reaper-theme', theme);
    }}),
    document.getElementById('wrapper')
  );
});
