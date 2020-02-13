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
import moment from "moment";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender, CFsCountListRender} from "jsx/mixin";
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import Button from 'react-bootstrap/lib/Button';
import Modal from 'react-bootstrap/lib/Modal';
import segmentList from 'jsx/segment-list'

const TableRow = CreateReactClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  _viewSegments: function(id) {
    console.log("Segments for run " + id );
    this.props.showSegments(id);
  },

  segmentsButton: function(id) {
    return <button className="btn btn-xs btn-info" onClick={() => this._viewSegments(id)}>View segments</button>
  },

  render: function() {
    let progressStyle = {
      marginTop: "0.25em",
      marginBottom: "0.25em"
    }
    let startTime = null;
    if(this.props.row.start_time) {
      startTime = moment(this.props.row.start_time).format("LLL");
    }
    const rowID = `#details_${this.props.row.id}`;
    const progressID = `#progress_${this.props.row.id}`;
    const segsRepaired = this.props.row.segments_repaired;
    const segsTotal = this.props.row.total_segments;
    const segsPerc = (100/segsTotal)*segsRepaired;
    const state = this.props.row.state;
    let etaOrDuration = moment(this.props.row.estimated_time_of_arrival).from(moment(this.props.row.current_time));
    if (!(state == 'RUNNING' || state == 'PAUSED')) {
      etaOrDuration = this.props.row.duration;
    } else if (segsPerc < 5) {
      etaOrDuration = 'TBD';
    }

    let progressStyleColor = "success";
    if (state == 'PAUSED') {
      progressStyleColor = "info";
    }
    else if (state != 'DONE' && state != 'RUNNING'){
      progressStyleColor = "danger";
    }

    const btnStartStop = (this.props.row.state === 'ABORTED' ? null : this.statusUpdateButton());
    const btnAbort = (state === 'RUNNING') || (state === 'PAUSED' ? this.abortButton() : this.deleteButton());
    const btnSegment = this.segmentsButton(this.props.row.id);
    const progressNow = Math.round((segsRepaired * 100) / segsTotal)
    const progressLabel = `${segsRepaired} / ${segsTotal}`

    const repairProgress = (
      <ProgressBar
        now={progressNow}
        active={state === 'RUNNING' ? 1 : 0}
        bsstyle={progressStyleColor}
        label={progressLabel}
      />
    );


    return (
    <tr>
        <td data-toggle="collapse" data-target={rowID}>{startTime}</td>
        <td data-toggle="collapse" data-target={rowID}>{etaOrDuration}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.state}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.cluster_name}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.keyspace_name}</td>
        <td data-toggle="collapse" data-target={rowID}><CFsCountListRender list={this.props.row.column_families} /></td>
        <td data-toggle="collapse" data-target={rowID}>
          <div className="progress">
          {repairProgress}
          </div>
        </td>
        <td>
          {btnSegment}
          {btnStartStop}
          {btnAbort}
        </td>
    </tr>
    );
  }

});

const TableRowDetails = CreateReactClass({
  getInitialState() {
      return { intensity: this.props.row.intensity };
  },

  _handleIntensityChange: function(e) {
      const value = e.target.value;
      const state = this.state;
      state["intensity"] = value;
      this.replaceState(state);

      this.props.updateIntensitySubject.onNext({id: this.props.row.id, intensity: value});
  },

  render: function() {

    const rowID = `details_${this.props.row.id}`;
    const createdAt = moment(this.props.row.creation_time).format("LLL");
    let startTime = null;
    if(this.props.row.start_time) {
      startTime = moment(this.props.row.start_time).format("LLL");
    }
    let endTime = null;
    if(this.props.row.end_time 
      && this.props.row.state != 'RUNNING' 
      && this.props.row.state != 'PAUSED'
      && this.props.row.state != 'NOT_STARTED') {
      if (this.props.row.state == 'ABORTED') {
        endTime = moment(this.props.row.pause_time).format("LLL");
      } else {
        endTime = moment(this.props.row.end_time).format("LLL");
      }
    }
    let pauseTime = null;
    if(this.props.row.pause_time
      && this.props.row.state != 'RUNNING' 
      && this.props.row.state != 'NOT_STARTED') {
      pauseTime = moment(this.props.row.pause_time).format("LLL");
    }
    
    let duration = this.props.row.duration;


    const incremental = this.props.row.incremental_repair == true ? "true" : "false";

    let intensity = this.props.row.intensity;
    if (this.props.row.state === 'PAUSED' && !!this.props.updateIntensitySubject) {
      intensity =
        <input
          type="number"
          className="form-control"
          value={this.state.intensity}
          min="0" max="1"
          onChange={this._handleIntensityChange}
          id={`${rowID}_in_intensity`}
          placeholder="repair intensity" />;
    }

    var metrics = ["io.cassandrareaper.service.RepairRunner.repairProgress", "io.cassandrareaper.service.RepairRunner.segmentsDone",
                   "io.cassandrareaper.service.RepairRunner.segmentsTotal", "io.cassandrareaper.service.RepairRunner.millisSinceLastRepair"];
  let cleanupRegex = /[^A-Za-z0-9]/mg
  let availableMetrics = metrics.map(metric => <div key={metric + this.props.row.repair_unit_id}>
      {metric + "." 
        + this.props.row.cluster_name.replace(cleanupRegex, "") + "." 
        + this.props.row.keyspace_name.replace(cleanupRegex, "") + "." 
        + this.props.row.repair_unit_id.replace(cleanupRegex, "")}</div>)
    return (
      <tr id={rowID} className="collapse out">
        <td colSpan="7">
          <table className="table table-condensed">
            <tbody>
                <tr>
                    <td>ID</td>
                    <td>{this.props.row.id}</td>
                </tr>
                <tr>
                    <td>Owner</td>
                    <td>{this.props.row.owner}</td>
                </tr>
                <tr>
                    <td>Cause</td>
                    <td>{this.props.row.cause}</td>
                </tr>
                <tr>
                    <td>Last event</td>
                    <td>{this.props.row.last_event}</td>
                </tr>
                <tr>
                    <td>Start time</td>
                    <td>{startTime}</td>
                </tr>
                <tr>
                    <td>End time</td>
                    <td>{endTime}</td>
                </tr>
                <tr>
                    <td>Pause time</td>
                    <td>{pauseTime}</td>
                </tr>
                <tr>
                    <td>Duration</td>
                    <td>{duration}</td>
                </tr>
                <tr>
                    <td>Segment count</td>
                    <td>{this.props.row.total_segments}</td>
                </tr>
                <tr>
                    <td>Segment repaired</td>
                    <td>{this.props.row.segments_repaired}</td>
                </tr>
                <tr>
                    <td>Intensity</td>
                    <td>{intensity}</td>
                </tr>
                <tr>
                    <td>Repair parallelism</td>
                    <td>{this.props.row.repair_parallelism}</td>
                </tr>
                <tr>
                    <td>Incremental repair</td>
                    <td>{incremental}</td>
                </tr>
                <tr>
                    <td>Repair threads</td>
                    <td>{this.props.row.repair_thread_count}</td>
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
                    <td>Blacklist</td>
                    <td><CFsListRender list={this.props.row.blacklisted_tables} /></td>
                </tr>
                <tr>
                    <td>Creation time</td>
                    <td>{createdAt}</td>
                </tr>
                <tr>
                    <td>Available metrics<br/><i>(can require a full run before appearing)</i></td>
                    <td>{availableMetrics}</td>
                </tr>
            </tbody>
          </table>
        </td>
      </tr>
    );
  },

});

const repairList = CreateReactClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    repairs: PropTypes.object.isRequired,
    clusterNames: PropTypes.object.isRequired,
    deleteSubject: PropTypes.object.isRequired,
    deleteResult: PropTypes.object.isRequired,
    updateStatusSubject: PropTypes.object.isRequired,
    updateIntensitySubject: PropTypes.object.isRequired,
    currentCluster: PropTypes.string.isRequired,
    changeCurrentCluster: PropTypes.func.isRequired
  },

  getInitialState: function() {
    return {
      repairs: null,
      deleteResultMsg: null,
      clusterNames: [],
      currentCluster: this.props.currentCluster,
      currentClusterSelectValue: {value: 'all', label: 'all'},
      runningCollapsed: false,
      doneCollapsed: false,
      modalShow: false,
      repairRunId: '',
      height: 0,
      width: 0,
      numberOfElementsToDisplay: 10,
      numberOfElementsToDisplaySelectValue: {value: 10, label: '10'},
    };
  },

  UNSAFE_componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );

    this._repairsSubscription = this.props.repairs.subscribeOnNext();

    this._repairRunSubscription = this.props.repairRunResult.subscribeOnNext(obs =>
      obs.subscribeOnNext(repairs => {
        this.setState({repairs: repairs});
      })
    );

    window.addEventListener('resize', this.updateWindowDimensions);
    this.updateWindowDimensions();
  },

  componentDidMount: function() {
   var intervalId = setInterval(this._handleTimer, 2000);
  },

  componentWillUnmount: function() {
    this._clustersSubscription.dispose();
    this._repairsSubscription.dispose();
    this._repairRunSubscription.dispose();
    window.removeEventListener('resize', this.updateWindowDimensions);
  },

  _handleTimer: function() {
      numberOfElementsToDisplay: 10,
     this.props.repairRunSubject.onNext({ clusterName: this.state.currentCluster, limit: this.state.numberOfElementsToDisplay });
  },

  updateWindowDimensions: function() {
    this.setState({ width: window.innerWidth, height: window.innerHeight });
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

  _toggleRunningDisplay: function() {
    if(this.state.runningCollapsed == true) {
      this.setState({runningCollapsed: false});
    }
    else {
      this.setState({runningCollapsed: true});
    }
  },

  _toggleDoneDisplay: function() {
    if(this.state.doneCollapsed == true) {
      this.setState({doneCollapsed: false});
    }
    else {
      this.setState({doneCollapsed: true});
    }
  },

  _displaySegments: function(repairRunId) {
    console.log("Displaying segments for run " + repairRunId)
    this.setState({ modalShow: true, repairRunId: repairRunId });
  },

  render: function() {

    let rowsRunning = null;
    let rowsDone = null;

    function compareEndTimeReverse(a,b) {
      if (a.end_time < b.end_time)
        return 1;
      if (a.end_time > b.end_time)
        return -1;
      return 0;
    }

    function compareStartTimeReverse(a,b) {
      if (a.start_time < b.start_time)
        return 1;
      if (a.start_time > b.start_time)
        return -1;
      return 0;
    }

    const modalClose = () => this.setState({ modalShow: false, repairRunId: ''});

    const segmentModal = (
      <Modal show={this.state.modalShow} onHide={modalClose} height={this.state.height} width={this.state.width} size="lg" aria-labelledby="contained-modal-title-lg" dialogClassName="large-modal">
        <Modal.Header closeButton>
          <Modal.Title id="contained-modal-title-lg">Segments</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <iframe src={"segments.html?repairRunId="+this.state.repairRunId} width="100%" height={parseInt(this.state.height)-200} frameBorder="0"/>
        </Modal.Body>
        <Modal.Footer>
          <Button onClick={modalClose}>Close</Button>
        </Modal.Footer>
      </Modal>
    );

    if (this.state.repairs !== null) {
        rowsRunning = this.state.repairs.sort(compareStartTimeReverse)
            .filter(repair => this.state.currentCluster === "all" || this.state.currentCluster === repair.cluster_name)
            .filter(repair => (repair.state === "RUNNING" || repair.state === "PAUSED" || repair.state === "NOT_STARTED"))
            .map(repair =>
                <tbody key={repair.id + '-rows'}>
                    <TableRow row={repair}
                              deleteSubject={this.props.deleteSubject}
                              updateStatusSubject={this.props.updateStatusSubject} showSegments={this._displaySegments}/>
                    <TableRowDetails row={repair}
                                     updateIntensitySubject={this.props.updateIntensitySubject}/>
                </tbody>
            );

        rowsDone = this.state.repairs.sort(compareEndTimeReverse)
            .filter(repair => this.state.currentCluster === "all" || this.state.currentCluster === repair.cluster_name)
            .filter(repair => (repair.state !== "RUNNING" && repair.state !== "PAUSED" && repair.state !== "NOT_STARTED"))
            .slice(0, this.state.numberOfElementsToDisplay)
            .map(repair =>
                <tbody key={repair.id + '-rows'}>
                    <TableRow row={repair}
                              deleteSubject={this.props.deleteSubject}
                              updateStatusSubject={this.props.updateStatusSubject} showSegments={this._displaySegments}/>
                    <TableRowDetails row={repair}/>
                </tbody>
            );
    }

    let selectClusterItems = this.state.clusterNames.sort().map(name => {
        { return { value: name, label: name}; }
    });

    selectClusterItems.unshift({value: 'all', label: 'all'});

    const clusterFilter = <form className="form-horizontal form-condensed">
            <div className="form-group">
              <label htmlFor="in_currentCluster" className="col-sm-3 control-label">Filter cluster:</label>
              <div className="col-sm-7 col-md-5 col-lg-3">
                <Select
                    id="in_currentCluster"
                    name="in_currentCluster"
                    classNamePrefix="select"
                    options={selectClusterItems}
                    value={this.state.currentClusterSelectValue}
                    onChange={this._handleSelectOnChange}
                />
              </div>
              <label htmlFor="in_numberOfElementsToDisplay" className="col-sm-1 control-label">Display:</label>
              <div className="col-sm-5 col-md-3 col-lg-1">
                <Select
                    id="in_numberOfElementsToDisplay"
                    name="in_numberOfElementsToDisplay"
                    classNamePrefix="select"
                    options={[
                        {value: 10, label: "10"},
                        {value: 25, label: "25"},
                        {value: 50, label: "50"},
                        {value: 100, label: "100"},
                    ]}
                    value={this.state.numberOfElementsToDisplaySelectValue}
                    onChange={this._handleSelectOnChange}
                />
              </div>
            </div>
    </form>

    let tableRunning = null;
    if(rowsRunning === null) {
        tableRunning = <div className="clusterLoader"></div>;
    } else if(rowsRunning.length === 0) {
      tableRunning = <div className="alert alert-info" role="alert">No running repair runs found</div>
    } else {
      tableRunning = <div className="row">
          <div className="col-sm-12">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>Start</th>
                              <th>ETA</th>
                              <th>State</th>
                              <th>Cluster</th>
                              <th>Keyspace</th>
                              <th>Tables</th>
                              <th>Repaired</th>
                              <th></th>
                          </tr>
                      </thead>
                        {rowsRunning}
                  </table>
              </div>
          </div>
      </div>;
    }

    let tableDone = null;
    if(rowsDone === null) {
      tableDone = <div className="clusterLoader"></div>;
    } else if(rowsDone.length === 0) {
      tableDone = <div className="alert alert-info" role="alert">No past repair runs found</div>
    } else {
      tableDone = <div className="row">
          <div className="col-sm-12">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>Start</th>
                              <th>Duration</th>
                              <th>State</th>
                              <th>Cluster</th>
                              <th>Keyspace</th>
                              <th>Tables</th>
                              <th>Repaired</th>
                              <th></th>
                          </tr>
                      </thead>
                        {rowsDone}
                  </table>
              </div>
          </div>
      </div>;
    }

    let menuRunningDownStyle = {
      display: "none" 
    }

    let menuRunningUpStyle = {
      display: "inline-block" 
    }

    if(this.state.runningCollapsed == true) {
      menuRunningDownStyle = {
        display: "inline-block"
      }
      menuRunningUpStyle = {
        display: "none"
      }
    }


    let menuDoneDownStyle = {
      display: "inline-block" 
    }

    let menuDoneUpStyle = {
      display: "none" 
    }

    if(this.state.doneCollapsed == true) {
      menuDoneDownStyle = {
        display: "none"
      }
      menuDoneUpStyle = {
        display: "inline-block"
      }
    }

    const runningHeader = <div className="panel-title"><a href="#repairs-running" data-toggle="collapse" onClick={this._toggleRunningDisplay}>Running&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuRunningDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuRunningUpStyle}></span></a></div>
    const doneHeader = <div className="panel-title"><a href="#repairs-done" data-toggle="collapse" onClick={this._toggleDoneDisplay}>Done&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDoneDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuDoneUpStyle}></span></a></div>

    return (
            <div>
              {segmentModal}
              {clusterFilter}
              <div className="panel panel-success">
                <div className="panel-heading">
                  {runningHeader}
                </div>
                <div className="panel-body collapse in" id="repairs-running">
                  {tableRunning}
                </div>
              </div>
              <div className="panel panel-info">
                <div className="panel-heading">
                  {doneHeader}
                </div>
                <div className="panel-body collapse" id="repairs-done">
                  {tableDone}
                </div>
              </div>
            </div>);
  }
});


export default repairList;
