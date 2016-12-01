import jQuery from "jquery";
import React from "react";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import RepairForm from "jsx/repair-form";
import RepairList from "jsx/repair-list";
import {
  statusObservableTimer,
  repairs,
  addRepairSubject, addRepairResult,
  deleteRepairSubject, deleteRepairResult, updateRepairStatusSubject,
  clusterNames
} from "observable";

jQuery(document).ready(function($){

  React.render(
    React.createElement(ServerStatus, {statusObservableTimer}),
    document.getElementById('cr-server-status')
  );

  React.render(
    React.createElement(Sidebar, {clusterNames}),
    document.getElementById('cr-sidebar')
  );

  React.render(
    React.createElement(RepairForm, {clusterNames, addRepairSubject, addRepairResult}),
    document.getElementById('cr-repair-form')
  );

  React.render(
    React.createElement(RepairList, {
      repairs,
      deleteSubject: deleteRepairSubject, deleteResult: deleteRepairResult,
      updateStatusSubject: updateRepairStatusSubject
    }),
    document.getElementById('cr-repair-list')
  );
});