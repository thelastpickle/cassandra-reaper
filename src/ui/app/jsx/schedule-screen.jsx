//
//  Copyright 2017-2019 The Last Pickle Ltd
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

import React from "react";
import CreateReactClass from 'create-react-class';
import PropTypes from 'prop-types';
import moment from "moment";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import RepairForm from "jsx/repair-form";
import ScheduleList from "jsx/schedule-list";
import NavBar from "jsx/navbar";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const ScheduleScreen = CreateReactClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  propTypes: {
    schedules: PropTypes.object.isRequired,
    clusterNames: PropTypes.object.isRequired,
    deleteSubject: PropTypes.object.isRequired,
    updateStatusSubject: PropTypes.object.isRequired,
    deleteResult: PropTypes.object.isRequired,
    currentCluster: PropTypes.string.isRequired,
    statusObservableTimer: PropTypes.object.isRequired,
    logoutSubject: PropTypes.object.isRequired,
    logoutResult: PropTypes.object.isRequired,
    switchTheme: PropTypes.func
  },

  getInitialState: function() {
    return {currentCluster:this.props.currentCluster=="undefined"?"all":this.props.currentCluster};
  },

  changeCurrentCluster : function(clusterName){
    this.setState({currentCluster: clusterName});
  },

  render: function() {

  const navStyle = {
    marginBottom: 0
  };

    return (
    <div id="wrapper">
        <nav className="navbar navbar-inverse navbar-static-top" role="navigation" style={navStyle}>
          <NavBar switchTheme={this.props.switchTheme}></NavBar>

            <Sidebar clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster}
              logoutSubject={this.props.logoutSubject} logoutResult={this.props.logoutResult}> </Sidebar>
        </nav>

        <div id="page-wrapper">
            <div className="row">
                <ServerStatus statusObservableTimer={this.props.statusObservableTimer}></ServerStatus>
            </div>

            <div className="row">
                <div className="col-lg-12">
                    <h1 className="page-header">Schedules</h1>
                </div>
            </div>

      <div className="row">
                <div className="col-lg-12">
                  <RepairForm addRepairSubject={this.props.addRepairSubject} addRepairResult={this.props.addRepairResult} clusterNames={this.props.clusterNames} currentCluster={this.props.currentCluster} formType="schedule"> </RepairForm>
                </div>
            </div>

            <div className="row">
                <div className="col-lg-12">
                  <ScheduleList schedules={this.props.schedules}
                              clusterNames={this.props.clusterNames}
                              deleteSubject={this.props.deleteSubject}
                              deleteResult={this.props.deleteResult}
                              updateStatusSubject={this.props.updateStatusSubject}
                              currentCluster={this.state.currentCluster}
                              changeCurrentCluster={this.changeCurrentCluster}> </ScheduleList>
                </div>
            </div>

        </div>
    </div>
    );
  }

});



export default ScheduleScreen;
