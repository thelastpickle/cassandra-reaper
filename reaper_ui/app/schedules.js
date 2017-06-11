import jQuery from "jquery";
import React from "react";
import ReactDOM from "react-dom";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import ScheduleScreen from "jsx/schedule-screen";
import ScheduleList from "jsx/schedule-list";
import {
  statusObservableTimer,
  addScheduleSubject, addScheduleResult, deleteScheduleSubject, deleteScheduleResult,
  updateScheduleStatusSubject, updateScheduleStatusResult,
  schedules, clusterNames
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

  ReactDOM.render(
    React.createElement(ScheduleScreen, {clusterNames, addScheduleSubject, addScheduleResult, currentCluster, schedules, deleteSubject: deleteScheduleSubject,
    deleteResult: deleteScheduleResult, updateStatusSubject: updateScheduleStatusSubject, statusObservableTimer}),
    document.getElementById('wrapper')
  );
});