//
//  Copyright 2015-2016 Stefan Podkowinski
//  Copyright 2016-2019 The Last Pickle Ltd
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
import { getUrlPrefix } from "jsx/mixin";
import $ from "jquery";
import DatePicker from "react-datepicker";
import Moment from 'moment';
import moment from "moment";
import Modal from 'react-bootstrap/lib/Modal';
import Button from 'react-bootstrap/lib/Button';

Moment.locale(navigator.language);

const repairForm = CreateReactClass({

  propTypes: {
    addRepairSubject: PropTypes.object.isRequired,
    addRepairResult: PropTypes.object.isRequired,
    clusterNames: PropTypes.object.isRequired,
    currentCluster: PropTypes.string.isRequired,
    formType: PropTypes.string.isRequired,
  },

  getInitialState: function() {
    const URL_PREFIX = getUrlPrefix(window.top.location.pathname);

    return {
      formType: this.props.formType,
      addRepairResultMsg: null,
      addRepairResultStatus: null,
      clusterNames: [],
      submitEnabled: false,
      clusterName: this.props.currentCluster === "all" ? this.props.clusterNames[0] : this.props.currentCluster,
      keyspace: "",
      tables: "",
      blacklistedTables: "",
      nodes: "",
      datacenters: "",
      owner: "",
      segments: "",
      parallelism: null,
      intensity: undefined,
      cause: "",
      startTime: this.props.formType === "schedule" ? moment().toDate() : null,
      intervalDays: "",
      incrementalRepair: "false",
      repairThreadCount: 1,
      timeout: null,
      formCollapsed: true,
      advancedFormCollapsed: true,
      clusterStatus: {},
      clusterTables: {},
      urlPrefix: URL_PREFIX,
      keyspaceOptions: [],
      tableOptions: [],
      nodeOptions: [],
      datacenterOptions: [],
      keyspaceList: [],
      keyspaceSelectValues: [],
      tablesList: [],
      tablesSelectValues: [],
      tablesSelectDisabled: false,
      blacklistedTablesList: [],
      blacklistedTablesSelectValues: [],
      blacklistedTablesSelectDisabled: false,
      nodesList: [],
      nodesSelectValues: [],
      nodesSelectDisabled: false,
      datacentersList: [],
      datacentersSelectValues: [],
      datacentersSelectDisabled: false,
      showModal: false,
      force: "false",
      adaptive: false,
      percentUnrepairedThreshold: ""
    };
  },

  UNSAFE_componentWillMount: function() {
    this._repairResultSubscription = this.props.addRepairResult.subscribeOnNext(obs =>
      obs.subscribe(
        r => this.setState({
          addRepairResultMsg: null,
          addRepairResultStatus: null,
          showModal: false,
          force: "false",
        }),
        r => this.setState({
          addRepairResultMsg: r.responseText,
          addRepairResultStatus: r.status,
          showModal: true,
        })
      )
    );

    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => {
        let previousNames = this.state.clusterNames;
        this.setState({clusterNames: names});
        if (names.length && !this.state.clusterName) {
          // Set the cluster name in state if it's not set yet
          this.setState({clusterName: names[0]});
        }
        if (!previousNames.length) {
          this._getClusterStatus();
        }
      })
    );
  },

  componentWillUnmount: function() {
    this._repairResultSubscription.dispose();
    this._clusterNamesSubscription.dispose();
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
          this.component._getDatacenterOptions();
          this.component._getNodeOptions();
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
    }
  },

  _getDatacenterOptions: function() {
    this.setState({
      datacenterOptions: Object.keys(this.state.clusterStatus.nodes_status.endpointStates[0].endpoints).sort().map(
        obj => { return {value: obj, label: obj}; }
      )
    });
  },

  _getNodeOptions: function() {
    this.setState({
      nodeOptions: this.state.clusterStatus.nodes_status.endpointStates[0].endpointNames.sort().map(
        obj => { return {value: obj, label: obj}; }
      )
    });
  },

  _getKeyspaceOptions: function() {
    this.setState({keyspaceOptions: Object.keys(this.state.clusterTables).map(
      obj => { return {value: obj, label: obj}; }
    )});
  },

  _getTableOptions: function(ks) {
    let tableOptionsRef = [];
    if (ks) {
      tableOptionsRef = this.state.clusterTables[ks].map(
        obj => { return {value: obj, label: obj}; }
      );
    }

    this.setState({tableOptions: tableOptionsRef});
  },

  _onAdd: function(e) {
    let repair = {
      clusterName: this.state.clusterName,
      keyspace: this.state.keyspace,
      owner: this.state.owner
    };

    if (this.state.formType === "schedule") {
      repair["scheduleTriggerTime"] = moment(this.state.startTime).utc().format("YYYY-MM-DDTHH:mm");
      repair["scheduleDaysBetween"] = this.state.intervalDays;
    }

    if (this.state.tables) repair.tables = this.state.tables;
    if (this.state.segments) repair.segmentCountPerNode = this.state.segments;
    if (this.state.parallelism) repair.repairParallelism = this.state.parallelism;
    if (this.state.intensity) repair.intensity = this.state.intensity;
    if (this.state.cause) repair.cause = this.state.cause;
    if (this.state.incrementalRepair) {
      repair.incrementalRepair = this.state.incrementalRepair;
      if (repair.incrementalRepair == "true") {
        repair.repairParallelism = "PARALLEL";
      }
    }
    else {
      repair.incrementalRepair = "false";
    }
    if (this.state.nodes) repair.nodes = this.state.nodes;
    if (this.state.datacenters) repair.datacenters = this.state.datacenters;
    if (this.state.blacklistedTables) repair.blacklistedTables = this.state.blacklistedTables;
    if (this.state.repairThreadCount && this.state.repairThreadCount > 0) repair.repairThreadCount = this.state.repairThreadCount;
    if (this.state.force) {
      repair.force = this.state.force;
    }
    else {
      repair.force = "false";
    }
    if (this.state.timeout) {
      repair.timeout = this.state.timeout;
    }
    if (this.state.adaptive) {
      repair.adaptive = this.state.adaptive;
    }
    else {
      repair.adaptive = "false";
    }

    if (this.state.percentUnrepairedThreshold) {
      repair.percentUnrepairedThreshold = this.state.percentUnrepairedThreshold;
    }
    else {
      repair.percentUnrepairedThreshold = "-1";
    }

    this.props.addRepairSubject.onNext({
      type: this.state.formType,
      params: repair,
    });
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.split("_")[1];

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    // validate
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.keyspaceList.length
      && this.state.clusterName
      && this.state.owner
      && ((this.state.formType === "schedule" && this.state.startTime && this.state.intervalDays) || this.state.formType != "schedule")
      && ((this.state.datacentersList.length && !this.state.nodesList.length)
          || (!this.state.datacentersList.length && this.state.nodesList.length)
          || (!this.state.datacentersList.length && !this.state.nodesList.length))
      ;
    
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

  _toggleAdvancedSettingsDisplay: function() {
    if(this.state.advancedFormCollapsed == true) {
      this.setState({advancedFormCollapsed: false});
    }
    else {
      this.setState({advancedFormCollapsed: true});
    }
  },

  _handleSelectOnChange: function(valueContext, actionContext) {
    const stateName = actionContext.name.split("_")[1];
    let stateValue = {};

    stateValue[stateName] = valueContext.value;
    this.setState(stateValue, ()=>this._handleSelectOnChangeCallback(stateName));
  },

  _handleSelectOnChangeCallback: function(stateName) {
    if (stateName === "clusterName") {
      this._getClusterStatus();
    }

    this._checkValidity();
  },

  _getOppositeSelect: function(selectName) {
    const selectNameMap = {
      "tables": "blacklistedTables",
      "blacklistedTables": "tables",
      "nodes": "datacenters",
      "datacenters": "nodes",
      "intervalDays": "percentUnrepairedThreshold",
      "percentUnrepairedThreshold": "intervalDays"
    };
    return selectNameMap[selectName];
  },

  _handleLinkedSelectOnChange: function(valueContext, actionContext) {
    const nameRef = actionContext.name.split("_")[1];
    const oppositeNameRef = this._getOppositeSelect(nameRef);
    const oppositeNameListRef = `${oppositeNameRef}List`;

    if (this.state[oppositeNameListRef].length) {
      return;
    }

    const nameListRef = `${nameRef}List`;
    const nameSelectValuesRef = `${nameRef}SelectValues`;
    const oppositeNameDisabledRef = `${oppositeNameRef}SelectDisabled`;

    let newList = this.state[nameListRef];
    let newSelectValues = [];
    let newRef = "";

    newList.length = 0;

    if (valueContext) {
      newList = valueContext.map(
        obj => { return {id: this._create_UUID(), text: obj.value}; }
      );
      newRef = valueContext.map(
        obj => { return obj.value; }
      ).join(",");
      newSelectValues = valueContext.map(
        obj => { return {label: obj.value, value: obj.value}; }
      );
    }

    let newState = {};
    newState[nameRef] = newRef;
    newState[nameListRef] = newList;
    newState[nameSelectValuesRef] = newSelectValues;
    newState[oppositeNameDisabledRef] = newList.length > 0;

    this.setState(newState);
    this._checkValidity();
  },

  // Keyspace list functions
  _handleKeyspaceSelectOnChange: function(valueContext, actionContext) {
    let keyspaceListRef = this.state.keyspaceList;
    let keyspaceSelectValuesRef = [];
    let keyspaceRef = "";

    keyspaceListRef.length = 0;

    if (valueContext) {
      keyspaceListRef.push({
        id: this._create_UUID(),
        text: valueContext.value
      });
      keyspaceRef = valueContext.value;
      keyspaceSelectValuesRef.push({
        label: valueContext.value, value: valueContext.value
      });
    }

    this.setState({
      keyspace: keyspaceRef,
      keyspaceList: keyspaceListRef,
      keyspaceSelectValues: keyspaceSelectValuesRef,
    });
    this._checkValidity();
    this._handleLinkedSelectOnChange(null, {name: "in_tables"});
    this._handleLinkedSelectOnChange(null, {name: "in_blacklistedTables"});
    this._getTableOptions(keyspaceRef);
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

  _onForce(e){
    this.setState({
      force: "true",
      addRepairResultMsg: null,
      showModal: false }, () => this._onAdd(e));
  },

  _onClose(e){
    this.setState({
      force: "false",
      addRepairResultMsg: null,
      showModal: false
    });
  },

  render: function() {

    let addMsg = null;
    if(this.state.addRepairResultMsg) {
      if(this.state.addRepairResultStatus && this.state.addRepairResultStatus === 409) {
        addMsg = (
                <span>
                <Modal show={this.state.showModal} onHide={this._onClose}>
                  <Modal.Header closeButton>
                    <Modal.Title>Scheduling conflict detected</Modal.Title>
                  </Modal.Header>
                  <Modal.Body>
                    <p>{this.state.addRepairResultMsg}</p>
                    <p>It is not reccommended to create overlapping repair schedules/runs.</p>
                    <p>For Cassandra 4.0 and later, you can force creating this schedule/run by clicking the "Force" button below.</p>
                  </Modal.Body>
                  <Modal.Footer>
                    <Button variant="secondary" onClick={this._onClose}>Cancel</Button>
                    <Button variant="primary" onClick={this._onForce}>Force</Button>
                  </Modal.Footer>
                </Modal>
              </span>
        );
      }
      else {
        addMsg = <div className="alert alert-danger" role="alert">{this.state.addRepairResultMsg}</div>
      }
    }

    const clusterNameOptions = this.state.clusterNames.sort().map(name => {
      return {label: name, value: name};
    });

    const clusterNamePlaceholder = this.state.clusterName ? this.state.clusterName : "Select cluster";
    const ownerPlaceholder = `owner name for the ${this.state.formType} run (any string)`;

    let formHeaderLabel = "Start a new repair";
    let repairButtonLabel = "Repair";
    let repairButtonClassName = "btn btn-warning";

    if (this.state.formType === "schedule") {
      formHeaderLabel = "Add a new schedule";
      repairButtonLabel = "Add Schedule";
      repairButtonClassName = "btn btn-success";
    }

    switch (this.state.formType) {
      case "repair":
        formHeaderLabel = "Start a new repair";

        break;
      case "schedule":
        formHeaderLabel = "Add a new schedule";
        break;
    }

    let advancedMenuDownStyle = {
      display: "inline-block" 
    }

    let advancedMenuUpStyle = {
      display: "none" 
    }

    if (this.state.advancedFormCollapsed == false) {
      advancedMenuDownStyle = {
        display: "none"
      }
      advancedMenuUpStyle = {
        display: "inline-block"
      }
    }

    let customInput = "";
    if (this.state.formType === "repair") {
      customInput = (
        <div className="form-group">
          <label htmlFor="in_cause" className="col-sm-3 control-label">Cause</label>
          <div className="col-sm-9 col-md-7 col-lg-5">
            <input type="text" className="form-control" value={this.state.cause}
              onChange={this._handleChange} id="in_cause" placeholder="reason repair was started"/>
          </div>
        </div>
      );
    }
    else if (this.state.formType === "schedule") {
      customInput = (
        <div>
          <div className="form-group">
            <label htmlFor="in_startTime" className="col-sm-3 control-label">Start time*</label>
            <div className="col-sm-9 col-md-7 col-lg-5">
              <DatePicker
                selected={this.state.startTime}
                onChange={value => this.setState({startTime: value})}
                showTimeSelect
                timeFormat="HH:mm"
                timeIntervals={15}
                timeCaption="Time"
                dateFormat="d MMMM yyyy HH:mm"
              />
            </div>
          </div>
          <div className="form-group">
            <label htmlFor="in_intervalDays" className="col-sm-3 control-label">Interval in days*</label>
            <div className="col-sm-9 col-md-7 col-lg-5">
              <input type="number" required className="form-control" value={this.state.intervalDays}
                onChange={this._handleChange} id="in_intervalDays" placeholder="amount of days to wait between scheduling new repairs, (e.g. 7 for weekly)"/>
            </div>
          </div>
          <div className="form-group">
            <label htmlFor="in_percentUnrepairedThreshold" className="col-sm-3 control-label">Percent unrepaired threshold</label>
            <div className="col-sm-9 col-md-7 col-lg-5">
              <input type="number" required className="form-control" value={this.state.percentUnrepairedThreshold}
                onChange={this._handleChange} id="in_percentUnrepairedThreshold" placeholder="% of unrepaired data over which repair should be started (optional)"/>
            </div>
          </div>
          <div className="form-group">
            <label htmlFor="in_adaptive" className="col-sm-3 control-label">Adaptive</label>
            <div className="col-sm-9 col-md-7 col-lg-5">
              <Select
                id="in_adaptive"
                name="in_adaptive"
                classNamePrefix="select"
                options={[{label: "true", value: "true"}, {label: "false", value: "false"}]}
                placeholder="false"
                onChange={this._handleSelectOnChange}
              />
            </div>
          </div>
        </div>
        
      );
    }

    const advancedSettingsHeader = (
      <div className="panel-title" >
        <a href="#advanced-form" data-toggle="collapse" onClick={this._toggleAdvancedSettingsDisplay}>
          Advanced settings&nbsp;
          <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={advancedMenuDownStyle}></span>
          <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={advancedMenuUpStyle}></span>
        </a>
      </div>
    );

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
            <label htmlFor="in_keyspace" className="col-sm-3 control-label">Keyspace*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <Select
                  id="in_keyspace"
                  name="in_keyspace"
                  classNamePrefix="select"
                  isClearable
                  isSearchable
                  options={this.state.keyspaceOptions}
                  value={this.state.keyspaceSelectValues}
                  placeholder="Add a keyspace"
                  onChange={this._handleKeyspaceSelectOnChange}
                />
              </div>
            </div>
            
            <div className="form-group">
              <label htmlFor="in_owner" className="col-sm-3 control-label">Owner*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.owner}
                  onChange={this._handleChange} id="in_owner" placeholder={ownerPlaceholder}/>
              </div>
            </div>
            {customInput}
            <div className="form-group">
              <div className="col-sm-offset-1 col-sm-9">
                <div className="panel panel-info">
                  <div className="panel-heading">
                    {advancedSettingsHeader}
                  </div>
                  <div className="panel-body collapse" id="advanced-form">
                    <div className="form-group">
                    <label htmlFor="in_tables" className="col-sm-3 control-label">Tables</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <Select
                          id="in_tables"
                          name="in_tables"
                          classNamePrefix="select"
                          isClearable
                          isSearchable
                          isMulti
                          isDisabled={this.state.tablesSelectDisabled}
                          options={this.state.tableOptions}
                          value={this.state.tablesSelectValues}
                          placeholder="Add a table (optional)"
                          onChange={this._handleLinkedSelectOnChange}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                    <label htmlFor="in_blacklist" className="col-sm-3 control-label">Blacklist</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <Select
                          id="in_blacklistedTables"
                          name="in_blacklistedTables"
                          classNamePrefix="select"
                          isClearable
                          isSearchable
                          isMulti
                          isDisabled={this.state.blacklistedTablesSelectDisabled}
                          options={this.state.tableOptions}
                          value={this.state.blacklistedTablesSelectValues}
                          placeholder="Add a table (optional)"
                          onChange={this._handleLinkedSelectOnChange}
                        />
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
                          isDisabled={this.state.nodesSelectDisabled}
                          options={this.state.nodeOptions}
                          value={this.state.nodesSelectValues}
                          placeholder="Add a node (optional)"
                          onChange={this._handleLinkedSelectOnChange}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_datacenters" className="col-sm-3 control-label">Datacenters</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <Select
                          id="in_datacenters"
                          name="in_datacenters"
                          classNamePrefix="select"
                          isClearable
                          isSearchable
                          isMulti
                          isDisabled={this.state.datacentersSelectDisabled}
                          options={this.state.datacenterOptions}
                          value={this.state.datacentersSelectValues}
                          placeholder="Add a datacenter (optional)"
                          onChange={this._handleLinkedSelectOnChange}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_segments" className="col-sm-3 control-label">Segments per node</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <input type="number" className="form-control" value={this.state.segments}
                          onChange={this._handleChange} id="in_segments" placeholder="amount of segments per node to create for the repair run"/>
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_parallelism" className="col-sm-3 control-label">Parallelism</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <Select
                          id="in_parallelism"
                          name="in_parallelism"
                          classNamePrefix="select"
                          options={[
                            {label: "Sequential", value: "SEQUENTIAL"},
                            {label: "Parallel", value: "PARALLEL"},
                            {label: "DC-Aware", value: "DATACENTER_AWARE"},
                          ]}
                          placeholder="Select parallelism"
                          onChange={this._handleSelectOnChange}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_intensity" className="col-sm-3 control-label">Repair intensity</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <input type="number" className="form-control" value={this.state.intensity}
                          min="0" max="1"
                          onChange={this._handleChange} id="in_intensity" placeholder="repair intensity"/>
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_incrementalRepair" className="col-sm-3 control-label">Incremental</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <Select
                          id="in_incrementalRepair"
                          name="in_incrementalRepair"
                          classNamePrefix="select"
                          options={[{label: "true", value: "true"}, {label: "false", value: "false"}]}
                          placeholder="false"
                          onChange={this._handleSelectOnChange}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_repairThreadCount" className="col-sm-3 control-label">Repair threads</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <input type="number" className="form-control" value={this.state.repairThreadCount}
                          min="1" max="4"
                          onChange={this._handleChange} id="in_repairThreadCount" placeholder="repair threads"/>
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_timeout" className="col-sm-3 control-label">Segment timeout</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                      <input type="number" className="form-control" value={this.state.timeout}
                          onChange={this._handleChange} id="in_timeout" placeholder="Segment timeout in minutes before it gets killed and rescheduled."/>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <div className="form-group">
              <div className="col-sm-offset-3 col-sm-9">
                <button type="button" className={repairButtonClassName} disabled={!this.state.submitEnabled}
                  onClick={this._onAdd}>{repairButtonLabel}</button>
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
        <a href="#repair-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>
          {formHeaderLabel}&nbsp;
          <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span>
          <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span>
        </a>
      </div>
    );

    return (
      <div className="panel panel-warning">
        <div className="panel-heading">
          {formHeader}
        </div>
        <div className="panel-body collapse" id="repair-form">
          {addMsg}
          {form}
        </div>
      </div>
    );
  }
});

export default repairForm;
