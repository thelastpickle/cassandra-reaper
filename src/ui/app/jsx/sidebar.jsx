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

import React from "react";

const sidebar = React.createClass({
  propTypes: {
    currentCluster: React.PropTypes.string.isRequired,
    clusterNames: React.PropTypes.object,
    logoutSubject: React.PropTypes.object.isRequired,
    logoutResult: React.PropTypes.object.isRequired
  },

  getInitialState: function () {
    return {logoutResultMsg: null};
  },

  componentWillMount: function () {
    this._logoutResultSubscription = this.props.logoutResult.subscribeOnNext(obs =>
        obs.subscribe(
        response => {
            this.setState({logoutResultMsg: null});
            window.location.href = "/webui/login.html";
        },
        response => {
            this.setState({logoutResultMsg: response.responseText});
        }
        )
    );
  },

  componentWillUnmount: function () {
    this._logoutResultSubscription.dispose();
  },

  _onLogout: function(e) {
    this.props.logoutSubject.onNext();
  },

  render: function() {
    const sideBarStyle = {marginTop: 0};

    let logoutError = null;
    if (this.state.logoutResultMsg) {
      logoutError = <div className="alert alert-danger" role="alert">{this.state.logoutResultMsg}</div>
    }

    return (
      <ul className="navbar-nav bg-gradient-primary sidebar sidebar-dark accordion" id="accordionSidebar">
        <li className="nav-item active">
          <a className="nav-link" href={'index.html?currentCluster=' + this.props.currentCluster}>
            <i className="fa fa-sitemap fa-fw"></i>
            <span>Clusters</span></a>
        </li>
        <li className="nav-item active">
          <a className="nav-link" href={'schedules.html?currentCluster=' + this.props.currentCluster}>
            <i className="fa fa-calendar fa-fw"></i>
            <span>Schedules</span></a>
        </li>
        <li className="nav-item active">
          <a className="nav-link" href={'repair.html?currentCluster=' + this.props.currentCluster}>
            <i className="fa fa-wrench fa-fw"></i>
            <span>Repairs</span></a>
        </li>
        <li className="nav-item active">
          <a className="nav-link" href={'snapshot.html?currentCluster=' + this.props.currentCluster}>
            <i className="fa fa-camera fa-fw"></i>
            <span>Snapshots</span></a>
        </li>
        <li className="nav-item active">
          <a className="nav-link" href="#" onClick={this._onLogout}>
            <i className="fa fa-sign-out fa-fw"></i>
            <span>Logout</span></a>
        </li>
        {logoutError}
      </ul>
    );
  }
});

export default sidebar;
