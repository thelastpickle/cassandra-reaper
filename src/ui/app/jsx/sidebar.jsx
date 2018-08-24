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
      <div className="navbar-default sidebar" style={sideBarStyle} role="navigation">
          <div className="sidebar-nav navbar-collapse">
              <ul className="nav" id="side-menu">
                  <li>
                      <a href={'index.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-sitemap fa-fw"></i> Clusters</a>
                  </li>
                  <li className="active">
                      <a href={'schedules.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-calendar fa-fw"></i> Schedules</a>
                  </li>
                  <li>
                      <a href={'repair.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-wrench fa-fw"></i> Repairs</a>
                  </li>
                  <li>
                      <a href={'snapshot.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-camera fa-fw"></i> Snapshots</a>
                  </li>
                  <li>
                      <a href="#" onClick={this._onLogout}><i className="fa fa-sign-out fa-fw"></i> Logout</a>
                  </li>
              </ul>
              {logoutError}
          </div>
      </div>
    );
  }
});

export default sidebar;
