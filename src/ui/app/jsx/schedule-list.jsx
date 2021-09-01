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
import CreateReactClass from 'create-react-class';
import PropTypes from 'prop-types';
import Select from 'react-select';
import moment from "moment";
import Modal from 'react-bootstrap/lib/Modal';
import Button from 'react-bootstrap/lib/Button';
import {
  CFsListRender,
  DeleteStatusMessageMixin,
  getUrlPrefix,
  RowDeleteMixin,
  StatusUpdateMixin,
  toast
} from "jsx/mixin";
import RepairScheduleForm from './repair-schedule-form'

var NotificationSystem = require('react-notification-system');

const EditRowModal = CreateReactClass({
  propTypes: {
    row: PropTypes.object,
    onCancel: PropTypes.func,
    onSave: PropTypes.func
  },

  getInitialState: function() {
    return {
      editFormState: {},
      canSubmit: false
    }
  },

  cancel: function() {
    if (this.props.onCancel) {
      this.props.onCancel();
    }
  },

  save: function() {
    if (this.props.onSave) {
      this.props.onSave(this.state.editFormState);
    }
  },

  onChangeHandler: function(e) {
    if (!e || !e.field) {
      return;
    }
    if (e && e.state) {
      if (e.state.valid) {
        this.setState({editFormState: e.state});
      }
      this.setState({canSubmit: e.state.valid});
    }
  },

  render: function () {
    return (
      <Modal show={this.props.row ? true : false} onHide={this.cancel}>
        <Modal.Header closeButton>
          <Modal.Title>Edit Schedule: {this.props.row ? this.props.row.cluster_name : 'Unknown Cluster'}/{this.props.row ? this.props.row.keyspace_name : 'Unknown Keyspace'}</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <div>
            <RepairScheduleForm repair={this.props.row} formType="schedule" onChange={this.onChangeHandler}/>
          </div>
        </Modal.Body>
        <Modal.Footer>
          <Button onClick={this.save} disabled={!this.state.canSubmit}>Save</Button>
          <Button onClick={this.cancel}>Cancel</Button>
        </Modal.Footer>
      </Modal>
    )
  }
});

const TableRow = CreateReactClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin],
  propTypes: {
    notificationSystem: PropTypes.object.isRequired,
    editScheduleCallback: PropTypes.func.isRequired
  },

  _runNow: function() {
    toast(this.props.notificationSystem, "Starting repair run for schedule " + this.props.row.id , "warning", this.props.row.id);
    $.ajax({
      url: getUrlPrefix(window.top.location.pathname) + '/repair_schedule/start/' + encodeURIComponent(this.props.row.id),
      method: 'POST',
      component: this,
      dataType: 'text',
      success: function(data) {
        toast(this.component.props.notificationSystem, "Repair run for schedule " + this.component.props.row.id + " will start shortly.", "success", this.component.props.row.id);
      },
      error: function(data) {
        toast(this.component.props.notificationSystem, "Failed starting repair run for schedule : " + data.responseText , "error", this.component.props.row.id);
      }
    });
  },

  _editSchedule: function() {
    this.props.editScheduleCallback(this.props.row);
  },

  render: function() {

    const next = moment(this.props.row.next_activation).fromNow();
    const rowID = `#details_${this.props.row.id}`;
    const incremental = this.props.row.incremental_repair == true ? "true" : "false";

    return (
    <tr>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.state}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.cluster_name}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.keyspace_name}</td>
        <td data-toggle="collapse" data-target={rowID}><CFsListRender list={this.props.row.column_families} /></td>
        <td data-toggle="collapse" data-target={rowID}><CFsListRender list={this.props.row.blacklisted_tables} /></td>
        <td data-toggle="collapse" data-target={rowID}>{incremental}</td>
        <td data-toggle="collapse" data-target={rowID}>{next}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.scheduled_days_between} days</td>
        <td>
          {this.statusUpdateButton()}
          {this.deleteButton()}
          <button type="button" className="btn btn-xs btn-info" onClick={this._runNow}>Run now</button>
          <button type="button" className="btn btn-xs btn-primary" onClick={this._editSchedule}>Edit</button>
        </td>
    </tr>
    );
  }
});


const TableRowDetails = CreateReactClass({
  render: function() {

    const createdAt = moment(this.props.row.creation_time).format("LLL");
    const nextAt = moment(this.props.row.next_activation).format("LLL");
    const rowID = `details_${this.props.row.id}`;
    const incremental = this.props.row.incremental_repair == true ? "true" : "false";

    let segmentCount = <tr>
                        <td>Segment count per node</td>
                        <td>{this.props.row.segment_count_per_node}</td>
                      </tr>;
    
    if (this.props.row.segment_count > 0) {
      segmentCount = <tr>
                      <td>Global segment count</td>
                      <td>{this.props.row.segment_count}</td>
                    </tr>;
    }

    return (
      <tr id={rowID} className="collapse out">
        <td colSpan="7">
          <table className="table table-condensed">
            <tbody>
                <tr>
                    <td>ID</td>
                    <td>{this.props.id}</td>
                </tr>
                <tr>
                    <td>Next run</td>
                    <td>{nextAt}</td>
                </tr>
                <tr>
                    <td>Owner</td>
                    <td>{this.props.row.owner}</td>
                </tr>
                <tr>
                    <td>Nodes</td>
                    <td><CFsListRender list={this.props.row.nodes} /></td>
                </tr>
                <tr>
                    <td>Datacenters</td>
                    <td><CFsListRender list={this.props.row.datacenters}/></td>
                </tr>
                <tr>
                    <td>Incremental</td>
                    <td>{incremental}</td>
                </tr>
                {segmentCount}
                <tr>
                    <td>Intensity</td>
                    <td>{this.props.row.intensity}</td>
                </tr>
                <tr>
                    <td>Repair threads</td>
                    <td>{this.props.row.repair_thread_count}</td>
                </tr>
                <tr>
                    <td>Repair parallelism</td>
                    <td>{this.props.row.repair_parallelism}</td>
                </tr>
                <tr>
                    <td>Pause time</td>
                    <td>{this.props.row.pause_time}</td>
                </tr>
                <tr>
                    <td>Creation time</td>
                    <td>{createdAt}</td>
                </tr>
            </tbody>
          </table>
        </td>
      </tr>
    );
  },

});


const scheduleList = CreateReactClass({
  mixins: [DeleteStatusMessageMixin],
  _notificationSystem: null,
  _editRow: undefined,

  propTypes: {
    schedules: PropTypes.object.isRequired,
    clusterNames: PropTypes.object.isRequired,
    deleteSubject: PropTypes.object.isRequired,
    updateStatusSubject: PropTypes.object.isRequired,
    deleteResult: PropTypes.object.isRequired,
    changeCurrentCluster: PropTypes.func.isRequired
  },

  getInitialState: function() {
    return {
      schedules: [],
      deleteResultMsg: null,
      clusterNames: [],
      currentClusterSelectValue: {value: 'all', label: 'all'},
      currentCluster: this.props.currentCluster
    };
  },

  UNSAFE_componentWillMount: function() {
    this._schedulesSubscription = this.props.schedules.subscribeOnNext(obs =>
      obs.subscribeOnNext(schedules => {
        const sortedSchedules = Array.from(schedules);
        sortedSchedules.sort((a, b) => a.id - b.id);
        this.setState({schedules: sortedSchedules});
      })
    );

    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );
  },

  componentDidMount: function() {
    this._notificationSystem = this.refs.notificationSystem;
  },

  componentWillUnmount: function() {
    this._schedulesSubscription.dispose();
    this._clustersSubscription.dispose();
  },

  _editSchedule: function(row) {
    this._editRow = row;
  },

  _saveSchedule: function(e) {
    console.log("_saveSchedule()");
    console.log(e);
    this._hideEditSchedule();
  },

  _hideEditSchedule: function() {
    this._editRow = undefined;
  },

  _handleSelectOnChange: function(valueContext, actionContext) {
    const nameRef = actionContext.name.split("_")[1];
    const nameSelectValueRef = `${nameRef}SelectValue`;

    let newSelectValue = {};
    let newValueRef = "";

    if (valueContext) {
        newSelectValue = valueContext;
        newValueRef = valueContext.value;
    }

    let newState = {};
    newState[nameRef] = newValueRef;
    newState[nameSelectValueRef] = newSelectValue;

    this.setState(newState);
    this.props.changeCurrentCluster(this.state.currentCluster);
  },

  render: function() {
    function compareNextActivationTime(a,b) {
      if (a.next_activation < b.next_activation)
        return -1;
      if (a.next_activation > b.next_activation)
        return 1;
      return 0;
    }

    let selectClusterItems = this.state.clusterNames.sort().map(name => {
        { return { value: name, label: name}; }
    });

    selectClusterItems.unshift({value: 'all', label: 'all'});

    const clusterFilter = <form className="form-horizontal form-condensed">
            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Filter cluster:</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <Select
                    id="in_currentCluster"
                    name="in_currentCluster"
                    classNamePrefix="select"
                    options={selectClusterItems}
                    value={this.state.currentClusterSelectValue}
                    onChange={this._handleSelectOnChange}
                />
              </div>
            </div>
    </form>

    const rows = this.state.schedules.sort(compareNextActivationTime).filter(schedule => this.state.currentCluster == "all" || this.state.currentCluster == schedule.cluster_name).map(schedule =>
      <tbody key={schedule.id+'-rows'}>
        <TableRow row={schedule} key={schedule.id+'-head'}
          deleteSubject={this.props.deleteSubject}
          updateStatusSubject={this.props.updateStatusSubject}
          notificationSystem={this._notificationSystem}
          editScheduleCallback={this._editSchedule}/>
        <TableRowDetails row={schedule} key={schedule.id+'-details'}/>
      </tbody>
    );

    let table = null;
    if(rows.length == 0) {
      table = <div className="alert alert-info" role="alert">No schedules found</div>
    } else {

      table = <div className="row">
          <div className="col-sm-12">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>State</th>
                              <th>Cluster</th>
                              <th>Keyspace</th>
                              <th>Tables</th>
                              <th>Blacklist</th>
                              <th>Incremental</th>
                              <th>Next run</th>
                              <th>Interval</th>
                              <th></th>
                          </tr>
                      </thead>
                        {rows}
                  </table>
              </div>
          </div>
      </div>;
    }

    return (<div className="panel panel-default">
              <div className="panel-body">
                <EditRowModal row={this._editRow} onCancel={this._hideEditSchedule} onSave={this._saveSchedule}/>
                <NotificationSystem ref="notificationSystem" />
                {this.deleteMessage()}
                {clusterFilter}
                {table}
              </div>
            </div>);
  }
});

export default scheduleList;
