//
//  Copyright 2017-2018 The Last Pickle Ltd
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
import moment from "moment";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import ScheduleForm from "jsx/schedule-form";
import ScheduleList from "jsx/schedule-list";
import NavBar from "jsx/navbar";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const ScheduleScreen = React.createClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  propTypes: {
    schedules: React.PropTypes.object.isRequired,
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    updateStatusSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    currentCluster: React.PropTypes.string.isRequired,
    statusObservableTimer: React.PropTypes.object.isRequired,
    logoutSubject: React.PropTypes.object.isRequired,
    logoutResult: React.PropTypes.object.isRequired
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
        <nav className="navbar navbar-default navbar-static-top" role="navigation" style={navStyle}>
            <NavBar></NavBar>

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
                  <ScheduleForm addScheduleSubject={this.props.addScheduleSubject} addScheduleResult={this.props.addScheduleResult} clusterNames={this.props.clusterNames} currentCluster={this.props.currentCluster}> </ScheduleForm>
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
