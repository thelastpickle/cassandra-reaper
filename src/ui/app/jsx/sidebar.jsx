import React from "react";
import { clusterSelected } from '../observable.js';

const sidebar = React.createClass({
  propTypes: {
    logoutSubject: React.PropTypes.object.isRequired,
    logoutResult: React.PropTypes.object.isRequired
  },

  getInitialState: function () {
    return {logoutResultMsg: null, currentCluster: null};
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
    this.selectClusterSubjectSubscription = clusterSelected.map(cluster => this.setState({currentCluster: cluster}));
  },

  componentWillUnmount: function () {
    this._logoutResultSubscription.dispose();
    this.selectClusterSubjectSubscription.dispose();
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
                      <a href={'index.html?currentCluster=' + this.state.currentCluster}><i className="fa fa-sitemap fa-fw"></i> Cluster</a>
                  </li>
                  <li className="active">
                      <a href={'schedules.html?currentCluster=' + this.state.currentCluster}><i className="fa fa-calendar fa-fw"></i> Schedules</a>
                  </li>
                  <li>
                      <a href={'repair.html?currentCluster=' + this.state.currentCluster}><i className="fa fa-wrench fa-fw"></i> Repair</a>
                  </li>
                  <li>
                      <a href={'snapshot.html?currentCluster=' + this.state.currentCluster}><i className="fa fa-camera fa-fw"></i> Snapshot</a>
                  </li>
                  <li>
                      <a href={'events.html?currentCluster=' + this.state.currentCluster}><i className="fa fa-search fa-fw"></i> Live Diagnostic</a>
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