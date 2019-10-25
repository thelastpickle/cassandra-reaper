//
//  Copyright 2018-2019 The Last Pickle Ltd
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
import SnapshotForm from "jsx/snapshot-form";
import SnapshotList from "jsx/snapshot-list";
import NavBar from "jsx/navbar";
import {CFsListRender, getUrlPrefix, humanFileSize, toastPermanent} from "jsx/mixin";
var NotificationSystem = require('react-notification-system');

const SnapshotScreen = React.createClass({
  _notificationSystem: null,
  propTypes: {
    clusterNames: React.PropTypes.object.isRequired,
    currentCluster: React.PropTypes.string.isRequired,
    switchTheme: React.PropTypes.func  
  },

  getInitialState: function() {
    return {currentCluster:this.props.currentCluster=="undefined"?"all":this.props.currentCluster,
            snapshots: {},  clusterNames:[],             
            snapshotsSizeOnDisk:{}, snapshotsTrueSize:{},
            totalSnapshotSizeOnDisk: 0, totalSnapshotTrueSize: 0,
            refreshEnabled: !(this.props.currentCluster=='all')};
  },

  componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );
  },

  componentWillUnmount: function() {
    this._clustersSubscription.dispose();
  },

  componentDidMount: function() {
    if(this.state.currentCluster!='all'){
        this._listSnapshots(this.state.currentCluster);
    }
    this._notificationSystem = this.refs.notificationSystem;
  },

  changeCurrentCluster : function(clusterName){
    this.setState({currentCluster: clusterName,
        refreshEnabled: !(clusterName=='all')});
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    // validate
    const valid = state.currentCluster;
    this.changeCurrentCluster(this.state.currentCluster);
    if (this.state.currentCluster != 'all') {
      this.setState({refreshEnabled: true});
      this._listSnapshots(this.state.currentCluster);
    } else {
      this.setState({refreshEnabled: false});
    }
  },

  _listSnapshots: function() {
    this.setState({communicating: true});
    $.ajax({
          url: getUrlPrefix(window.top.location.pathname) + '/snapshot/cluster/' + encodeURIComponent(this.state.currentCluster),
          method: 'GET',
          component: this,
          success: function(data) {
            let snapshots = data;
            this.component.setState({snapshotsSizeOnDisk:{}, snapshotsTrueSize:{},
              totalSnapshotSizeOnDisk: 0, totalSnapshotTrueSize: 0});
            this.component.setState({'snapshots': snapshots});
            let snapshotSizeOnDisk = {};
            let snapshotTrueSize = {};
            Object.keys(this.component.state.snapshots).sort().forEach(snapshotName => {
              snapshotSizeOnDisk[snapshotName] = 0;
              snapshotTrueSize[snapshotName] = 0;
              Object.keys(this.component.state.snapshots[snapshotName]).forEach(key => this.component.state.snapshots[snapshotName][key].forEach(
                table => {
                  snapshotSizeOnDisk[snapshotName]+=table.sizeOnDisk;
                  snapshotTrueSize[snapshotName]+=table.trueSize;
              
              }))
            });
            
            this.component.setState({snapshotsSizeOnDisk:snapshotSizeOnDisk, snapshotsTrueSize:snapshotTrueSize,
                  totalSnapshotSizeOnDisk: 0, totalSnapshotTrueSize: 0});
          },
          complete: function(data) {
            this.component.setState({communicating: false});
          },
          error: function(data) {
            toastPermanent(this.component._notificationSystem, "Error : " + data.responseText, "error", this.component.state.currentCluster);
          }
      });
  },

  render: function() {
    let progressStyle = {
        display: "none" 
    }

    let refreshStyle = {
        display: "inline-block" 
    }

    if (this.state.communicating==true) {
        progressStyle = {
            display: "inline-block"
        }
        refreshStyle = {
            display: "none"
        } 
    }

  const navStyle = {
    marginBottom: 0
  };

  const clusterItems = this.state.clusterNames.sort().map(name =>
    <option key={name} value={name}>{name}</option>
  );

  const clusterFilter = <form className="form-horizontal form-condensed">
  <div className="form-group">
    <label htmlFor="in_currentCluster" className="col-sm-3 control-label">Filter cluster:</label>
    <div className="col-sm-7 col-md-5 col-lg-3">
      <select className="form-control" id="in_currentCluster"
        onChange={this._handleChange} value={this.state.currentCluster}>
        <option key="all" value="all">Select a cluster...</option>
        {clusterItems}
      </select>
    </div>
    <div className="col-sm-5 col-md-3 col-lg-1">
    <button type="button" className="btn btn-success" style={refreshStyle}
        onClick={this._listSnapshots} disabled={!this.state.refreshEnabled}>Refresh</button>
    <button type="button" className="btn btn-success" style={progressStyle}
        disabled="true">Refreshing...</button>
    </div>
  </div>
</form>

    return (
        <div>
        <NotificationSystem ref="notificationSystem" />
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
                    <h1 className="page-header">Snapshots</h1>
                </div>
            </div>
            <div className="col-lg-12">
                  <SnapshotForm clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster}  changeCurrentCluster={this.changeCurrentCluster}
                                listSnapshots={this._listSnapshots}
                                changeCurrentCluster={this.changeCurrentCluster}> </SnapshotForm>
            </div>
            <div className="row">
                <div className="col-lg-12">
                {clusterFilter}
                  <SnapshotList clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster} changeCurrentCluster={this.changeCurrentCluster} snapshots={this.state.snapshots}
                                snapshotsSizeOnDisk={this.state.snapshotsSizeOnDisk} snapshotsTrueSize={this.state.snapshotsTrueSize}
                                totalSnapshotSizeOnDisk={this.state.totalSnapshotSizeOnDisk} totalSnapshotTrueSize={this.state.totalSnapshotTrueSize}
                                listSnapshots={this._listSnapshots}>
                  </SnapshotList>
                </div>
            </div>
        </div>
        </div>
    );
  }

});



export default SnapshotScreen;
