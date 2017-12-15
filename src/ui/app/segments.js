import jQuery from "jquery";
import ReactDOM from "react-dom";
import React from "react";
import ServerStatus from "jsx/server-status";
import SegmentList from "jsx/segment-list";
import {
  statusObservableTimer,
  addClusterSubject, addClusterResult, deleteClusterSubject, deleteClusterResult,
  clusterNames
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


  let repairRunId: string = $.urlParam('repairRunId');
  if(!repairRunId) {
    repairRunId = '0';
  } 

  ReactDOM.render(
    React.createElement(SegmentList, {repairRunId}),
    document.getElementById('wrapper')
  );
});