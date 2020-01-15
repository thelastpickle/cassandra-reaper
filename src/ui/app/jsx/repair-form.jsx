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


const repairForm = CreateReactClass({

  propTypes: {
    addRepairSubject: PropTypes.object.isRequired,
    addRepairResult: PropTypes.object.isRequired,
    clusterNames: PropTypes.object.isRequired
  },

  getInitialState: function() {
    const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
    
    return {
      addRepairResultMsg: null,
      clusterNames: [],
      submitEnabled: false,
      clusterName: this.props.currentCluster != "all" ? this.props.currentCluster : this.props.clusterNames[0],
      keyspace: "",
      tables: "",
      blacklistedTables: "",
      owner: null,
      segments: null,
      parallelism: null,
      intensity: null,
      cause: null,
      incrementalRepair: null,
      formCollapsed: true,
      nodes: "",
      datacenters: "",
      nodeList: [],
      datacenterList: [],
      clusterStatus: {},
      urlPrefix: URL_PREFIX,
      nodeSuggestions: [],
      datacenterSuggestions: [],
      clusterTables: {},
      tableList: [],
      tableOptions: [],
      tableSelectValues: [],
      tableSelectDisabled: false,
      blacklistList: [],
      blacklistSelectValues: [],
      blacklistSelectDisabled: false,
      keyspaceList: [],
      keyspaceOptions: [],
      keyspaceSelectValues: [],
      advancedFormCollapsed: true,
      repairThreadCount: 1
    };
  },

  componentWillMount: function() {
    this._repairResultSubscription = this.props.addRepairResult.subscribeOnNext(obs =>
      obs.subscribe(
        r => this.setState({addRepairResultMsg: null}),
        r => this.setState({addRepairResultMsg: r.responseText})
      )
    );

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
    this._repairResultSubscription.dispose();
    this._clusterNamesSubscription.dispose();
  },


  _getClusterStatus: function() {
    let clusterName = this.state.clusterName;

    if (clusterName !== null) {
      $.ajax({
          url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(clusterName),
          method: 'GET',
          component: this,
          complete: function(data) {
            this.component.setState({clusterStatus: $.parseJSON(data.responseText)});
            this.component._getSuggestions();
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

  _getSuggestions: function() {
    var nodes = this.state.clusterStatus.nodes_status.endpointStates[0].endpointNames;
    nodes.sort();
    this.state.nodeSuggestions = nodes;
    
    var datacenters = Object.keys(this.state.clusterStatus.nodes_status.endpointStates[0].endpoints);
    datacenters.sort;
    this.setState({datacenterSuggestions: datacenters});
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
    const repair = {
      clusterName: this.state.clusterName, keyspace: this.state.keyspace,
      owner: this.state.owner
    };
    if(this.state.tables) repair.tables = this.state.tables;
    if(this.state.segments) repair.segmentCount = this.state.segments;
    if(this.state.parallelism) repair.repairParallelism = this.state.parallelism;
    if(this.state.intensity) repair.intensity = this.state.intensity;
    if(this.state.cause) repair.cause = this.state.cause;
    if(this.state.incrementalRepair) repair.incrementalRepair = this.state.incrementalRepair;
    if(this.state.nodes) repair.nodes = this.state.nodes;
    if(this.state.datacenters) repair.datacenters = this.state.datacenters;
    if(this.state.blacklistedTables) repair.blacklistedTables = this.state.blacklistedTables;
    if(this.state.repairThreadCount && this.state.repairThreadCount > 0) repair.repairThreadCount = this.state.repairThreadCount;

    // Force incremental repair to FALSE if empty
    if(!this.state.incrementalRepair) repair.incrementalRepair = "false";

    this.props.addRepairSubject.onNext(repair);
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);
    
    if (n == 'clusterName') {
      this._getClusterStatus();
    }
    
    // validate
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.keyspaceList.length > 0 && this.state.clusterName && this.state.owner
                                      && ((this.state.datacenterList.length>0 && this.state.nodeList.length==0)
                                      || (this.state.datacenterList.length==0 && this.state.nodeList.length > 0) || (this.state.datacenterList.length==0  && this.state.nodeList==0) );
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

  _handleAddition(node) {
    if (this.state.datacenterList.length == 0 && node.length > 1) {
      let nodes = this.state.nodeList;
      if ($.inArray(node, this.state.nodes.split(','))==-1) {
        nodes.push({
            id: nodes.length + 1,
            text: node
        });
        this.setState({nodeList: nodes, nodes: nodes.map(node => node.text).join(',')});
        this._checkValidity();
      }
    }
  },
  
  _handleDelete(i) {
        let nodes = this.state.nodeList;
        nodes.splice(i, 1);
        this.setState({nodeList: nodes, nodes: nodes.map(node => node.text).join(',')});
        this._checkValidity();
  },

  _handleDcAddition(dc) {
    if (this.state.nodeList.length == 0 && dc.length > 1) {
      let datacenters = this.state.datacenterList;
      if ($.inArray(dc, this.state.datacenters.split(','))==-1) {
        datacenters.push({
            id: datacenters.length + 1,
            text: dc
        });
        this.setState({datacenterList: datacenters, datacenters: datacenters.map(dc => dc.text).join(',')});
        this._checkValidity();
      }
    }
  },
  
  _handleDcDelete(i) {
        let datacenters = this.state.datacenterList;
        datacenters.splice(i, 1);
        this.setState({datacenterList: datacenters, datacenters: datacenters.map(dc => dc.text).join(',')});
        this._checkValidity();
  },

  _handleNodeFilterSuggestions(textInputValue, possibleSuggestionsArray) {
    var lowerCaseQuery = textInputValue.toLowerCase();
    let nodes = this.state.nodes;
 
    return possibleSuggestionsArray.filter(function(suggestion)  {
        return suggestion.toLowerCase().includes(lowerCaseQuery) && $.inArray(suggestion, nodes.split(','))==-1;
    })
  },

  _handleDcFilterSuggestions(textInputValue, possibleSuggestionsArray) {
    var lowerCaseQuery = textInputValue.toLowerCase();
    let datacenters = this.state.datacenters;
 
    return possibleSuggestionsArray.filter(function(suggestion)  {
        return suggestion.toLowerCase().includes(lowerCaseQuery) && $.inArray(suggestion, datacenters.split(','))==-1;
    })
  },

    // Blacklist tag list functions
    _handleBlacklistSelectOnChange: function(valueContext, actionContext) {
      if (this.state.tableList.length) {
        return;
      }

      let blacklistListRef = this.state.blacklistList;
      let blacklistSelectValuesRef = [];
      let blacklistedTablesRef = "";

      blacklistListRef.length = 0;

      if (valueContext) {
        blacklistListRef = valueContext.map(
          obj => { return {id: this._create_UUID(), text: obj.value}; }
        );
        blacklistedTablesRef = valueContext.map(
          obj => { return obj.value; }
        ).join(",");
        blacklistSelectValuesRef = valueContext.map(
          obj => { return {label: obj.value, value: obj.value}; }
        );
      }

      this.setState({
        blacklistedTables: blacklistedTablesRef,
        blacklistList: blacklistListRef,
        blacklistSelectValues: blacklistSelectValuesRef,
      });
      this._checkValidity();
      this.setState({tableSelectDisabled: blacklistListRef.length > 0});
    },
  
    // Tables tag list functions
    _handleTableSelectOnChange: function(valueContext, actionContext) {
      if (this.state.blacklistList.length) {
        return;
      }

      let tableListRef = this.state.tableList;
      let tableSelectValuesRef = [];
      let tableRef = "";

      tableListRef.length = 0;

      if (valueContext) {
        tableListRef = valueContext.map(
          obj => { return {id: this._create_UUID(), text: obj.value}; }
        );
        tableRef = valueContext.map(obj => { return obj.value; }).join(",");
        tableSelectValuesRef = valueContext.map(
          obj => { return {label: obj.value, value: obj.value }; }
        );
      }

      this.setState({
        tables: tableRef,
        tableList: tableListRef,
        tableSelectValues: tableSelectValuesRef,
      });
      this._checkValidity();
      this.setState({blacklistSelectDisabled: tableListRef.length > 0});
    },
  
    // Keyspace list functions
    _handleKeySpaceSelectOnChange: function(valueContext, actionContext) {
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
      this._handleTableSelectOnChange(null, null);
      this._handleBlacklistSelectOnChange(null, null);
      this._getTableOptions(keyspaceRef);
    },

    _create_UUID(){
      var dt = new Date().getTime();
      var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
          var r = (dt + Math.random()*16)%16 | 0;
          dt = Math.floor(dt/16);
          return (c=='x' ? r :(r&0x3|0x8)).toString(16);
      });
      return uuid;
    },

  render: function() {

    let addMsg = null;
    if(this.state.addRepairResultMsg) {
      addMsg = <div className="alert alert-danger" role="alert">{this.state.addRepairResultMsg}</div>
    }

    const clusterItems = this.state.clusterNames.sort().map(name =>
      <option key={name} value={name}>{name}</option>
    );

    let advancedMenuDownStyle = {
      display: "inline-block" 
    }

    let advancedMenuUpStyle = {
      display: "none" 
    }

    if(this.state.advancedFormCollapsed == false) {
      advancedMenuDownStyle = {
        display: "none"
      }
      advancedMenuUpStyle = {
        display: "inline-block"
      }
    }

    const keyspaceInputStyle = this.state.keyspaceList.length > 0 ? 'form-control-hidden':'form-control';

    const advancedSettingsHeader = <div className="panel-title" >
    <a href="#advanced-form" data-toggle="collapse" onClick={this._toggleAdvancedSettingsDisplay}>Advanced settings
    &nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={advancedMenuDownStyle}></span>
           <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={advancedMenuUpStyle}></span></a></div>

    const form = <div className="row">
        <div className="col-lg-12">

          <form className="form-horizontal form-condensed">

            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Cluster*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_clusterName"
                  onChange={this._handleChange} value={this.state.clusterName}>
                  {clusterItems}
                </select>
              </div>
            </div>

            <div className="form-group">
            <label htmlFor="in_keyspace" className="col-sm-3 control-label">Keyspace*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <Select
                  id="in_keyspace"
                  name="in_keyspace"
                  isClearable
                  isSearchable
                  options={this.state.keyspaceOptions}
                  value={this.state.keyspaceSelectValues}
                  placeholder="Add a keyspace"
                  onChange={this._handleKeySpaceSelectOnChange}
                  classNames={{tagInputField: keyspaceInputStyle}}
                />
              </div>
            </div>
            
            <div className="form-group">
              <label htmlFor="in_owner" className="col-sm-3 control-label">Owner*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.owner}
                  onChange={this._handleChange} id="in_owner" placeholder="owner name for the repair run (any string)"/>
              </div>
            </div>
 

            <div className="form-group">
              <label htmlFor="in_cause" className="col-sm-3 control-label">Cause</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" className="form-control" value={this.state.cause}
                  onChange={this._handleChange} id="in_cause" placeholder="reason repair was started"/>
              </div>
            </div>

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
                          id="in_table"
                          name="in_table"
                          isClearable
                          isSearchable
                          isMulti
                          isDisabled={this.state.tableSelectDisabled}
                          options={this.state.tableOptions}
                          value={this.state.tableSelectValues}
                          placeholder="Add a table (optional)"
                          onChange={this._handleTableSelectOnChange}
                          classNames={{tagInputField: 'form-control'}}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                    <label htmlFor="in_blacklist" className="col-sm-3 control-label">Blacklist</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <Select
                          id="in_blacklist"
                          name="in_blacklist"
                          isClearable
                          isSearchable
                          isMulti
                          isDisabled={this.state.blacklistSelectDisabled}
                          options={this.state.tableOptions}
                          value={this.state.blacklistSelectValues}
                          placeholder="Add a table (optional)"
                          onChange={this._handleBlacklistSelectOnChange}
                          classNames={{tagInputField: 'form-control'}}
                        />
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_nodes" className="col-sm-3 control-label">Nodes</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <p>ReactTag node list was here</p>
                      </div>
                    </div>
                    <div className="form-group">
                      <label htmlFor="in_datacenters" className="col-sm-3 control-label">Datacenters</label>
                      <div className="col-sm-14 col-md-12 col-lg-9">
                        <p>ReactTag datacenter list was here</p>
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
                        <select className="form-control" id="in_parallelism"
                          onChange={this._handleChange} value={this.state.parallelism}>
                          <option value=""></option>
                          <option value="SEQUENTIAL">Sequential</option>
                          <option value="PARALLEL">Parallel</option>
                          <option value="DATACENTER_AWARE">DC-Aware</option>
                        </select>
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
                        <select className="form-control" id="in_incrementalRepair"
                          onChange={this._handleChange} value={this.state.incrementalRepair}>
                          <option value="false">false</option>
                          <option value="true">true</option>
                        </select>
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
                  </div>
                </div>
              </div>
            </div>
            <div className="form-group">
              <div className="col-sm-offset-3 col-sm-9">
                <button type="button" className="btn btn-warning" disabled={!this.state.submitEnabled}
                  onClick={this._onAdd}>Repair</button>
              </div>
            </div>            
          </form>

      </div>
    </div>

    

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

    const formHeader = <div className="panel-title" ><a href="#repair-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>Start a new repair&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span></a></div>

    return (<div className="panel panel-warning">
              <div className="panel-heading">
                {formHeader}
              </div>
              <div className="panel-body collapse" id="repair-form">
                {addMsg}
                {form}
              </div>
            </div>);
  }
});

export default repairForm;
