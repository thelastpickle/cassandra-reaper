//
//  Copyright 2018-2018 The Last Pickle Ltd
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
import Select from 'react-select';
import {getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";
var NotificationSystem = require('react-notification-system');

const snapshotForm = CreateReactClass({
  _notificationSystem: null,

  propTypes: {
    clusterNames: PropTypes.object.isRequired,
    listSnapshots: PropTypes.func,
    changeCurrentCluster: PropTypes.func
  },

  getInitialState: function() {
    const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
    
    return {
      submitEnabled: false,
      clusterNames: [],
      clusterName: this.props.currentCluster === "all" ? this.props.clusterNames[0] : this.props.currentCluster,
      keyspace: "",
      owner: "",
      name: "",
      cause: "",
      formCollapsed: true,
      clusterStatus: {},
      urlPrefix: URL_PREFIX,
      clusterTables: {},
      keyspaceList: [],
      keyspaceOptions: [],
    };
  },

  UNSAFE_componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => {
        let previousNames = this.state.clusterNames;
        this.setState({clusterNames: names});
        if(names.length == 1) this.setState({clusterName: names[0]});
        if(previousNames.length == 0) {
          this._getClusterStatus();
        }
      })
    );
  },

  componentWillUnmount: function() {
    this._clusterNamesSubscription.dispose();
  },

  componentDidMount: function() {
    this._notificationSystem = this.refs.notificationSystem;
  },

  _getClusterStatus: function() {
    let clusterName = this.state.clusterName;
    $.ajax({
      url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(clusterName),
      method: 'GET',
      component: this,
      complete: function(data) {
        this.component.setState({clusterStatus: $.parseJSON(data.responseText)});
      }
    });
    $.ajax({
      url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(clusterName) + '/tables',
      method: 'GET',
      component: this,
      complete: function(data) {
        this.component.setState({clusterTables: $.parseJSON(data.responseText)});
        this.component._getKeyspaceOptions();
      }
    });
  },

  _takeSnapshot: function(snapshot) {
    this.setState({communicating: true});
    toast(this._notificationSystem, "Taking a new snapshot...", "warning", snapshot.clusterName);
    const snapshotParams = '&snapshot_name='
      + encodeURIComponent(snapshot.name === '' ? 'reaper' : snapshot.name)
      + '&owner=' + encodeURIComponent(snapshot.owner)
      + '&cause=' + encodeURIComponent(snapshot.cause);
      
    $.ajax({
      url: getUrlPrefix(window.top.location.pathname) + '/snapshot/cluster/' + encodeURIComponent(snapshot.clusterName) + "?keyspace=" + snapshot.keyspace + snapshotParams,
      dataType: 'text',
      method: 'POST',
      component: this,
      success: function(data) {
        toast(this.component._notificationSystem, "Successfully took a new snapshot", "success", snapshot.clusterName);
      },
      complete: function(data) {
        this.component.setState({communicating: false});
        this.component.props.changeCurrentCluster(snapshot.clusterName);
        this.component.props.listSnapshots(snapshot.clusterName);
      },
      error: function(data) {
        toast(
          this.component._notificationSystem,
          "Failed taking a snapshot on cluster " + snapshot.clusterName + " : " + data.responseText,
          "error",
          snapshot.clusterName
        );
      }
    });
  },

  _getKeyspaceOptions: function() {
    this.setState({keyspaceOptions: Object.keys(this.state.clusterTables).map(
      obj => { return {value: obj, label: obj}; }
    )});
  },

  _onAdd: function(e) {
    const snapshot = {
      clusterName: this.state.clusterName, 
      owner: this.state.owner,
      name: this.state.name,
      keyspace: "",
    };
    if (this.state.keyspace) {
      snapshot["keyspace"] = this.state.keyspace;
    }
    if (this.state.cause) {
      snapshot["cause"] = this.state.cause;
    }

    this._takeSnapshot(snapshot);
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    // validate
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.clusterName && this.state.owner;
    this.setState({submitEnabled: valid});
  },

  _toggleFormDisplay: function() {
    if(this.state.formCollapsed == true) {
      this.setState({formCollapsed: false});
    }
    else {
      this.setState({formCollapsed: true});
    }
  },

  _handleSelectOnChange: function(valueContext, actionContext) {
    const stateName = actionContext.name.split("_")[1];
    let stateValue = {};

    stateValue[stateName] = valueContext.value;
    this.setState(stateValue);

    if (stateName === "clusterName") {
      this._getClusterStatus();
    }

    this._checkValidity();
  },

  _handleKeyspaceSelectOnChange: function(valueContext, actionContext) {
    let keyspaceListRef = this.state.keyspaceList;
    let keyspaceRef = "";

    keyspaceListRef.length = 0;

    if (valueContext) {
      keyspaceListRef.push({
        id: this._create_UUID(),
        text: valueContext.value
      });
      keyspaceRef = valueContext.value;
    }

    this.setState({
      keyspace: keyspaceRef,
      keyspaceList: keyspaceListRef,
    });
    this._checkValidity();
  },

  _create_UUID(){
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = (dt + Math.random()*16)%16 | 0;
      dt = Math.floor(dt/16);
      return (c === 'x' ? r : (r&0x3|0x8)).toString(16);
    });
    return uuid;
  },

  render: function() {
    const clusterNameOptions = this.state.clusterNames.sort().map(name => {
      return {label: name, value: name};
    });

    const clusterNamePlaceholder = this.state.clusterName ? this.state.clusterName : "Select cluster";

    const keyspaceInputStyle = this.state.keyspaceList.length ? 'form-control-hidden':'form-control';

    const form = (
      <div className="row">
        <div className="col-lg-12">

          <form className="form-horizontal form-condensed">

            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Cluster*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <Select
                  id="in_clusterName"
                  name="in_clusterName"
                  classNamePrefix="select"
                  options={clusterNameOptions}
                  placeholder={clusterNamePlaceholder}
                  onChange={this._handleSelectOnChange}
                />
              </div>
            </div>

            <div className="form-group">
              <label htmlFor="in_name" className="col-sm-3 control-label">Name</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.name}
                  onChange={this._handleChange} id="in_name" placeholder="name of the snapshot (any string)"/>
              </div>
            </div>

            <div className="form-group">
            <label htmlFor="in_keyspace" className="col-sm-3 control-label">Keyspace</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <Select
                  id="in_keyspace"
                  name="in_keyspace"
                  classNamePrefix="select"
                  isClearable
                  isSearchable
                  options={this.state.keyspaceOptions}
                  placeholder="select a keyspace otherwise all keyspaces are selected"
                  onChange={this._handleKeyspaceSelectOnChange}
                />
              </div>
            </div>

            <div className="form-group">
              <label htmlFor="in_owner" className="col-sm-3 control-label">Owner*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.owner}
                  onChange={this._handleChange} id="in_owner" placeholder="owner name for the snapshot (any string)"/>
              </div>
            </div>

            <div className="form-group">
              <label htmlFor="in_cause" className="col-sm-3 control-label">Cause</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" className="form-control" value={this.state.cause}
                  onChange={this._handleChange} id="in_cause" placeholder="reason snapshot was taken"/>
              </div>
            </div>

            <div className="form-group">
              <div className="col-sm-offset-3 col-sm-9">
                <button type="button" className="btn btn-warning" disabled={!this.state.submitEnabled}
                  onClick={this._onAdd}>Take snapshot</button>
              </div>
            </div>
          </form>
        </div>
      </div>
    );

    let menuDownStyle = {
      display: "inline-block" 
    }

    let menuUpStyle = {
      display: "none" 
    }

    if(this.state.formCollapsed == false) {
      menuDownStyle = {
        display: "none"
      }
      menuUpStyle = {
        display: "inline-block"
      }
    }

    const formHeader = (
      <div className="panel-title" >
        <a href="#snapshot-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>
          Take a snapshot&nbsp;
          <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span>
          <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span>
        </a>
      </div>
    );

    return (
      <div className="panel panel-warning">
        <div className="panel-heading">
          <NotificationSystem ref="notificationSystem" />
          {formHeader}
        </div>
        <div className="panel-body collapse" id="snapshot-form">
          {form}
        </div>
      </div>
    );
  }
});

export default snapshotForm;
