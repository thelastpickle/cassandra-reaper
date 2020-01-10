//
//  Copyright 2018-2018 Stefan Podkowinski
//  Copyright 2019-2019 The Last Pickle Ltd
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

import $ from "jquery";
import React from "react";
import CreateReactClass from 'create-react-class';
import PropTypes from 'prop-types';
import Select from 'react-select';
import {getUrlPrefix} from "jsx/mixin";


const subscriptionForm = CreateReactClass({

  propTypes: {
    addSubscriptionSubject: PropTypes.object.isRequired,
    listenSubscriptionSubject: PropTypes.object.isRequired,
    addSubscriptionResult: PropTypes.object.isRequired,
    clusterStatusResult: PropTypes.object.isRequired,
    clusterSelected: PropTypes.object.isRequired,
    clusterNames: PropTypes.object.isRequired
  },

  getInitialState: function(formExpanded) {
    const URL_PREFIX = getUrlPrefix(window.top.location.pathname);

    return {
      addSubscriptionResultMsg: null,
      submitEnabled: false,
      clusterName: null,
      clusterNames: [],
      description: "",
      export_sse: false,
      export_logger: "",
      export_http: "",
      nodes: "",
      nodeList: [],
      nodeOptions: [],
      events_selection: [],
      formCollapsed: !formExpanded,
      urlPrefix: URL_PREFIX
    };
  },

  UNSAFE_componentWillMount: function() {
    this._scheduleResultSubscription = this.props.addSubscriptionResult.subscribeOnNext(obs =>
      obs.subscribe(
        r => this.setState({addSubscriptionResultMsg: null}),
        r => this.setState({addSubscriptionResultMsg: r.responseText})
      )
    );

    this._clusterSelectedSubscription = this.props.clusterSelected.subscribeOnNext(name => {
      this.replaceState(this.getInitialState(!this.state.formCollapsed));
      // reset form elements not bound to state
      $("#in_events_selection").val([]);
    });

    this._clusterStatusResultSubscription = this.props.clusterStatusResult.subscribeOnNext(obs => {
      obs.subscribeOnNext(status => {
        this.setState({clusterName: status.name});
      });
    });

    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => {
        let previousNames = this.state.clusterNames;
        this.setState({clusterNames: names});
        if (names.length) this.setState({clusterName: names[0]});
        if (!previousNames.length) {
          this._getClusterStatus();
        }
      })
    );
  },

  componentWillUnmount: function() {
    this._scheduleResultSubscription.dispose();
    this._clusterSelectedSubscription.dispose();
    this._clusterStatusResultSubscription.dispose();
  },

  _onAdd: function(e) {
    const sub = {
      clusterName: this.state.clusterName,
      description: this.state.description,
      nodes: this.state.nodes,
      events: this.state.events_selection.join(","),
      exportSse: this.state.export_sse,
    };
    if (this.state.export_logger) {
      sub[exportFileLogger] = this.state.export_logger;
    }
    if (this.state.export_http) {
      sub[exportHttpEndpoint] = this.state.export_http;
    }

    this.props.addSubscriptionSubject.onNext(sub);
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix
    if (n === "export_sse") {
      v = e.target.checked === true;
    }

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    if (n === "clusterName") {
      this._getClusterStatus();
    }

    // validate
    this._checkValidity();
  },

  _onEventsSelection: function(element, checked) {
    const opts = element.target.selectedOptions;
    const ov = [];
    for(var i = 0; i < opts.length; i++) {
      ov.push(opts[i].value);
    }
    this.setState({events_selection: ov});
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.description
      && this.state.events_selection
      && this.state.events_selection.length
      && this.state.nodes
      && this.state.nodes.length
      && (
        this.state.export_sse
        || this.state.export_logger
        || (
          this.state.export_http
          && (
            this.state.export_http.startsWith("http://")
            || this.state.export_http.startsWith("https://")
          )
        )
      );

    this.setState({submitEnabled: valid});
  },

  _toggleFormDisplay: function() {
    if (this.state.formCollapsed) {
      this.setState({formCollapsed: false});
    }
    else {
      this.setState({formCollapsed: true});
    }
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

  _getClusterStatus: function() {
    let clusterName = this.state.clusterName;

    if (clusterName) {
      $.ajax({
          url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(clusterName),
          method: 'GET',
          component: this,
          complete: function(data) {
            this.component.setState({clusterStatus: $.parseJSON(data.responseText)});
            this.component._getNodeOptions();
          }
      });
    }
  },

  _getNodeOptions: function() {
    this.setState({
      nodeOptions: this.state.clusterStatus.nodes_status.endpointStates[0].endpointNames.map(
        obj => { return {value: obj, label: obj}; }
      )
    });
  },

  _handleSelectOnChange: function(valueContext, actionContext) {
    let nodeListRef = this.state.nodeList;
    let nodesRef = "";

    nodeListRef.length = 0;

    if (valueContext) {
      nodeListRef = valueContext.map(
        obj => { return {id: this._create_UUID(), text: obj.value}; }
      );
      nodesRef = valueContext.map(
        obj => { return obj.value; }
      ).join(",");
    }

    this.setState({
      nodeList: nodeListRef,
      nodes: nodesRef,
    });

    this._checkValidity();
  },

  render: function() {
    let addMsg = null;
    if(this.state.addSubscriptionResultMsg) {
      addMsg = (
        <div className="alert alert-danger" role="alert">
          {this.state.addSubscriptionResultMsg}
        </div>
      );
    }

    const form = (
      <div className="row">
        <div className="col-lg-12">

          <form className="form-horizontal form-condensed">
            <div className="col-sm-offset-1 col-sm-11">
            <div className="form-group">
                <label htmlFor="in_description" className="col-sm-3 control-label">Description</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="text" className="form-control" value={this.state.description}
                    onChange={this._handleChange} id="in_description" placeholder="Name of subscription"/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_nodes" className="col-sm-3 control-label">Nodes</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <Select
                    id="in_nodes"
                    name="in_nodes"
                    classNamePrefix="select"
                    isClearable
                    isSearchable
                    isMulti
                    placeholder="Add a node"
                    options={this.state.nodeOptions}
                    onChange={this._handleSelectOnChange}
                  />
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="events-selection" className="col-sm-3 control-label">Events:</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <select id="in_events_selection" multiple="multiple" onChange={this._onEventsSelection} className="form-control">
                    <option value="org.apache.cassandra.audit.AuditEvent">AuditEvent</option>
                    <option value="org.apache.cassandra.dht.BootstrapEvent">BootstrapEvent</option>
                    <option value="org.apache.cassandra.gms.GossiperEvent">GossiperEvent</option>
                    <option value="org.apache.cassandra.hints.HintEvent">HintEvent</option>
                    <option value="org.apache.cassandra.hints.HintsServiceEvent">HintsServiceEvent</option>
                    <option value="org.apache.cassandra.service.PendingRangeCalculatorServiceEvent">PendingRangeCalculatorServiceEvent</option>
                    <option value="org.apache.cassandra.schema.SchemaAnnouncementEvent">SchemaAnnouncementEvent</option>
                    <option value="org.apache.cassandra.schema.SchemaEvent">SchemaEvent</option>
                    <option value="org.apache.cassandra.schema.SchemaMigrationEvent">SchemaMigrationEvent</option>
                    <option value="org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent">TokenAllocatorEvent</option>
                    <option value="org.apache.cassandra.locator.TokenMetadataEvent">TokenMetadataEvent</option>
                  </select>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_export_sse" className="col-sm-3 control-label">Enable Live View</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="checkbox" id="in_export_sse" onChange={this._handleChange} value={this.state.export_sse}
                         checked={this.state.export_sse}/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_export_logger" className="col-sm-3 control-label">Export file logger</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="text" className="form-control" value={this.state.export_logger}
                    onChange={this._handleChange} id="in_export_logger" placeholder="Name of logger used for file export"/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_export_http" className="col-sm-3 control-label">Export HTTP endpoint</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="url" className="form-control" value={this.state.export_http}
                    onChange={this._handleChange} id="in_export_http" placeholder="URL endpoint used for posting events"/>
                </div>
              </div>
            </div>
            <div className="col-sm-offset-3 col-sm-9">
              <button type="button" className="btn btn-success" disabled={!this.state.submitEnabled}
                onClick={this._onAdd}>Save</button>
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
        <a href="#schedule-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>
          Add Events Subscription
        </a>
        &nbsp;
        <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span>
        <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span>
      </div>
    );

    return (
      <div className="panel panel-warning">
        <div className="panel-heading">
          {formHeader}
        </div>
        <div className="panel-body collapse" id="schedule-form">
          {addMsg}
          {form}
        </div>
      </div>
    );
  }
});

export default subscriptionForm;
