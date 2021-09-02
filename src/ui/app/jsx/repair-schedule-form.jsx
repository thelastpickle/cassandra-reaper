/*
 * Copyright 2021- DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import CreateReactClass from "create-react-class";
import React from "react";
import PropTypes from "prop-types";
import moment from "moment";
import {Collapse} from "react-bootstrap";
import Select from "react-select";

const RepairScheduleForm = CreateReactClass({
  propTypes: {
    repair: PropTypes.object,
    formType: PropTypes.string.isRequired,
    onChange: PropTypes.func  // Called fired when a valid change is detected within the form
  },

  getInitialState: function() {
    return {
      cluster: this.props.repair ? this.props.repair.cluster_name : '',
      keyspace: this.props.repair ? this.props.repair.keyspace_name : '',
      owner: this.props.repair ? this.props.repair.owner : '',
      startTime: this.props.repair ? this.props.repair.next_activation : (this.props.formType === 'schedule' ? moment().toDate() : null),
      intervalDays: this.props.repair ? this.props.repair.scheduled_days_between : '',
      tables: this.props.repair ? this.props.repair.column_families : [],
      tablesBlacklisted: this.props.repair ? this.props.repair.blacklisted_tables : [],
      nodes: this.props.repair ? this.props.repair.nodes : [],
      datacenters: this.props.repair ? this.props.repair.datacenters : [],
      segments: this.props.repair ? this.props.repair.segment_count_per_node : '',
      parallelism: this.props.repair ? this.props.repair.repair_parallelism : 'PARALLEL',
      intensity: this.props.repair ? this.props.repair.intensity : '',
      incrementalRepair: this.props.repair ? this.props.repair.incremental_repair : 'false',
      repairThreadCount: this.props.repair ? this.props.repair.repair_thread_count : 1,
      formType: this.props.formType,
      advancedSettingsOpen: false,
      valid: undefined  // Indicates if the current state of the form is valid or not
    }
  },

  formSelectChangeHandler: function(valueContext, actionContext) {
    const field = actionContext.name.split("_")[1];
    const value = valueContext.value;
    this.setStateFormChange(field, value);
  },
  
  formInputChangeHandler: function(e) {
    const field = e.target.id.split("_")[1];
    const value = e.target.value;
    this.setStateFormChange(field, value);
  },

  setStateFormChange: function(field, value) {
    if (!field || !field.length) {
      return;
    }

    const changeEvent = {
      field: field,
      value: value ? value.toString() : undefined,
      origValue: this.state && field && this.state[field] ? this.state[field].toString() : undefined
    }

    const state = this.state;
    if (field) {
      state[field] = value;
    }
    state.valid = this.validate(state);
    this.setState(state);

    changeEvent.state = state;
    if (this.props.onChange) {
      this.props.onChange(changeEvent);
    }
  },

  validate: function(state) {
    if (!state) {
      return false;
    }

    const clusterInvalid = !state.cluster || !state.cluster.length;
    const keyspaceInvalid = !state.keyspace || !state.keyspace.length;
    const ownerInvalid = !state.owner || !state.owner.length;
    const scheduleTimeInvalid = (state.formType === "schedule" && (!state.startTime || !state.intervalDays)) || (state.formType === "repair" && (state.startTime || state.intervalDays));
    // TODO What should the rules here be for the the combination of Datacenters and Nodes
    const datacentersSet = state.datacenters && state.datacenters.length ? true : false;
    const nodesSet = state.nodes && state.nodes.length ? true : false;
    const datacentersNodesInvalid = datacentersSet && nodesSet;

    const invalid = clusterInvalid || keyspaceInvalid || ownerInvalid || scheduleTimeInvalid || datacentersNodesInvalid;
    return !invalid;
  },

  toggleAdvancedSettings: function() {
    this.setState({advancedSettingsOpen: !this.state.advancedSettingsOpen});
  },

  render: function() {
    this.setStateFormChange();

    const ownerPlaceholder = `owner name for the ${this.state.formType} run (any string)`;

    const trueFalseOptions = [
      {label: "True", value: "true"},
      {label: "False", value: "false"}
    ];
    const incrementalRepairValue = trueFalseOptions.filter(value => value.value === this.state.incrementalRepair.toString())[0];

    const parallelismOptions = [
      {label: "Sequential", value: "SEQUENTIAL"},
      {label: "Parallel", value: "PARALLEL"},
      {label: "DC-Aware", value: "DATACENTER_AWARE"},
    ];
    const parallelismValue = parallelismOptions.filter(value => value.value === this.state.parallelism)[0];

    return (
      <form>
        {/* Cluster */}
        <div className="form-group">
          <label htmlFor="in_clusterName" className="control-label">Cluster*</label>
          <input type="text" required className="form-control" defaultValue={this.state.cluster} id="in_clusterName" disabled/>
        </div>

        {/* Keyspace */}
        <div className="form-group">
          <label htmlFor="in_keyspace" className="control-label">Keyspace*</label>
          <input type="text" required className="form-control" defaultValue={this.state.keyspace} id="in_keyspace" disabled/>
        </div>

        {/* Owner */}
        <div className="form-group">
          <label htmlFor="in_owner" className="control-label">Owner*</label>
          <input type="text" required className="form-control" defaultValue={this.state.owner} id="in_owner" placeholder={ownerPlaceholder} onChange={this.formInputChangeHandler}/>
        </div>

        {/* Start Time */}
        <div className="form-group">
          <label htmlFor="in_startTime" className="control-label">Start time*</label>
          <input type="text" required className="form-control" defaultValue={this.state.startTime} id="in_startTime" disabled/>
        </div>

        {/* Interval */}
        <div className="form-group">
          <label htmlFor="in_intervalDays" className="control-label">Interval in days*</label>
          <input type="number" min={1} required className="form-control" defaultValue={this.state.intervalDays} id="in_intervalDays" placeholder="The number of days to wait between scheduling new repairs, (e.g. 7 for weekly)" onChange={this.formInputChangeHandler}/>
        </div>

        <div className="form-group">
          <div className="panel panel-info">
            <div className="panel-heading">
              <div className="panel-title">
                <a onClick={this.toggleAdvancedSettings}>
                  <span style={{paddingRight: '.5em'}}>Advanced settings</span>
                  <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={ this.state.advancedSettingsOpen ? { display: 'none'} : {}}></span>
                  <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={ !this.state.advancedSettingsOpen ? { display: 'none'} : {}}></span>
                </a>
              </div>
            </div>
            <div className="panel-body" style={ !this.state.advancedSettingsOpen ? {padding: '0'} : {}}>
              <Collapse in={this.state.advancedSettingsOpen}>
                <div id="collapse-advanced-settings">

                  {/* Tables */}
                  <div className="form-group">
                    <label htmlFor="in_tables" className="control-label">Tables</label>
                    <input type="text" className="form-control" defaultValue={this.state.tables ? this.state.tables.join(', ') : ''} id="in_tables" disabled/>
                  </div>

                  {/* Blacklist */}
                  <div className="form-group">
                    <label htmlFor="in_blacklistedTables" className="control-label">Blacklist</label>
                    <input type="text" className="form-control" defaultValue={this.state.blacklistedTables ? this.state.blacklistedTables.join(', ') : ''} id="in_blacklistedTables" disabled/>
                  </div>

                  {/* Nodes */}
                  <div className="form-group">
                    <label htmlFor="in_nodes" className="control-label">Nodes</label>
                    <input type="text" className="form-control" defaultValue={this.state.nodes ? this.state.nodes.join(', ') : ''} id="in_nodes" disabled/>
                  </div>

                  {/* Datacenters */}
                  <div className="form-group">
                    <label htmlFor="in_datacenters" className="control-label">Datacenters</label>
                    <input type="text" className="form-control" defaultValue={this.state.datacenters ? this.state.datacenters.join(', ') : ''} id="in_datacenters" disabled/>
                  </div>

                  {/* Segments Per Node */}
                  <div className="form-group">
                    <label htmlFor="in_segments" className="control-label">Segments per node</label>
                    <input type="number" min="0" className="form-control" defaultValue={this.state.segments} id="in_segments" placeholder="Number of segments per node to create for the repair run" onChange={this.formInputChangeHandler}/>
                  </div>

                  {/* Parallelism */}
                  <div className="form-group">
                    <label htmlFor="in_parallelism" className="control-label">Parallelism</label>
                    <Select
                      id="in_parallelism"
                      name="in_parallelism"
                      classNamePrefix="select"
                      options={parallelismOptions}
                      value={parallelismValue}
                      onChange={this.formSelectChangeHandler}
                    />
                  </div>

                  {/* Intensity */}
                  <div className="form-group">
                    <label htmlFor="in_intensity" className="control-label">Repair Intensity</label>
                    <input type="number" min="0" max="1" className="form-control" defaultValue={this.state.intensity} id="in_intensity" onChange={this.formInputChangeHandler}/>
                  </div>

                  {/* Incremental */}
                  <div className="form-group">
                    <label htmlFor="in_incrementalRepair" className="control-label">Incremental</label>
                      <Select
                        id="in_incrementalRepair"
                        name="in_incrementalRepair"
                        classNamePrefix="select"
                        value={incrementalRepairValue}
                        options={trueFalseOptions}
                        isDisabled={true}
                      />
                  </div>

                  {/* Repair Threads */}
                  <div className="form-group">
                    <label htmlFor="in_repairThreadCount" className="control-label">Repair Threads</label>
                    <input type="number" min="1" className="form-control" defaultValue={this.state.repairThreadCount} id="in_repairThreadCount" disabled/>
                  </div>

                </div>
              </Collapse>
            </div>
          </div>
        </div>
      </form>
    )
  }
});

export default RepairScheduleForm;
