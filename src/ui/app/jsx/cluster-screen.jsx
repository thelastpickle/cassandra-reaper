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
import ClusterForm from "jsx/cluster-form";
import ClusterList from "jsx/cluster-list";
import NavBar from "jsx/navbar";
import LoginForm from "jsx/login-form";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const ClusterScreen = React.createClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  propTypes: {
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    currentCluster: React.PropTypes.string.isRequired,
    addClusterSubject: React.PropTypes.object.isRequired,
    addClusterResult:  React.PropTypes.object.isRequired,
    loginSubject: React.PropTypes.object.isRequired,
    loginResult: React.PropTypes.object.isRequired,
    logoutSubject: React.PropTypes.object.isRequired,
    logoutResult: React.PropTypes.object.isRequired

  },

  getInitialState: function() {
    return {
        currentCluster:this.props.currentCluster=="undefined"?"all":this.props.currentCluster
    }
  },

  changeCurrentCluster : function(clusterName){
    this.setState({currentCluster: clusterName});
  },

  render: function() {
  const navStyle = {
    marginBottom: 0
  };

  let content = 
    <div>
        <div className="col-lg-12">
            <ClusterForm clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster} addClusterSubject={this.props.addClusterSubject} addClusterResult={this.props.addClusterResult} > </ClusterForm>
        </div>
        <div className="row">
            <div className="col-lg-12">
                <ClusterList clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster} deleteSubject={this.props.deleteSubject} deleteResult={this.props.deleteResult} > </ClusterList>
            </div>
        </div>
    </div> 

  return (
        <div>
            <!-- Navigation -->
        <nav className="navbar navbar-default navbar-static-top" role="navigation" style={navStyle}>
            <NavBar></NavBar>
            <!-- /.navbar-header -->

            <Sidebar clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster} 
                loginSubject={this.props.loginSubject} loginResult={this.props.loginResult}
                logoutSubject={this.props.logoutSubject} logoutResult={this.props.logoutResult}> </Sidebar>
        </nav>

        <div id="page-wrapper">
            <div className="row">
                <ServerStatus statusObservableTimer={this.props.statusObservableTimer}></ServerStatus>
            </div>
            <div className="row">
                <div className="col-lg-12">
                    <h1 className="page-header">Cluster</h1>
                </div>
                <!-- /.col-lg-12 -->
            </div>
            <!-- /.row -->
            {content}
        </div>
        <!-- /#page-wrapper -->
        </div>
    );
  }

});



export default ClusterScreen;
