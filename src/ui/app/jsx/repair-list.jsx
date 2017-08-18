import React from "react";
import moment from "moment";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const TableRow = React.createClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  render: function() {

    const rowID = `#details_${this.props.row.id}`;
    const segsRepaired = this.props.row.segments_repaired;
    const segsTotal = this.props.row.total_segments;
    const segsPerc = (100/segsTotal)*segsRepaired;
    const incremental = this.props.row.incremental_repair == true ? "true" : "false";
    const state = this.props.row.state;
    const btnStartStop = this.props.row.state == 'ABORTED' ? null : this.statusUpdateButton();
    const btnAbort = state == 'RUNNING' || state == 'PAUSED' ? this.abortButton() : this.deleteButton();

    return (
    <tr>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.id}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.state}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.cluster_name}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.keyspace_name}</td>
        <td data-toggle="collapse" data-target={rowID}><CFsListRender list={this.props.row.column_families} /></td>
        <td data-toggle="collapse" data-target={rowID}>{incremental}</td>
        <td data-toggle="collapse" data-target={rowID}>
          <div className="progress">
            <div className="progress-bar" role="progressbar"
              aria-valuenow={segsRepaired} aria-valuemin="0"
              aria-valuemax={segsTotal}
              style={{width: segsPerc+'%'}}>
              {segsRepaired}/{segsTotal}
            </div>
          </div>
        </td>
        <td>
          {btnStartStop}
          {btnAbort}
        </td>
    </tr>
    );
  }

});

const TableRowDetails = React.createClass({
  render: function() {

    const rowID = `details_${this.props.row.id}`;
    const createdAt = moment(this.props.row.creation_time).format("LLL");
    let startTime = null;
    if(this.props.row.start_time) {
      startTime = moment(this.props.row.start_time).format("LLL");
    }
    let endTime = null;
    if(this.props.row.end_time) {
      endTime = moment(this.props.row.end_time).format("LLL");
    }
    let pauseTime = null;
    if(this.props.row.pause_time) {
      pauseTime = moment(this.props.row.pause_time).format("LLL");
    }

    const incremental = this.props.row.incremental_repair == true ? "true" : "false";

    return (
      <tr id={rowID} className="collapse out">
        <td colSpan="7">
          <table className="table table-condensed">
            <tbody>
                <tr>
                    <td>Owner</td>
                    <td>{this.props.row.owner}</td>
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
                    <td>{this.props.row.duration}</td>
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
                    <td>{this.props.row.intensity}</td>
                </tr>
                <tr>
                    <td>Repair parallism</td>
                    <td>{this.props.row.repair_parallelism}</td>
                </tr>
                <tr>
                    <td>Incremental repair</td>
                    <td>{incremental}</td>
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

const repairList = React.createClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    repairs: React.PropTypes.object.isRequired,
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    updateStatusSubject: React.PropTypes.object.isRequired,
    currentCluster: React.PropTypes.string.isRequired,
    changeCurrentCluster: React.PropTypes.func.isRequired

  },

  getInitialState: function() {
    return {repairs: [], deleteResultMsg: null, clusterNames:[], currentCluster:this.props.currentCluster, runningCollapsed: false, doneCollapsed: false};
  },

  componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );

    this._repairsSubscription = this.props.repairs.subscribeOnNext(obs =>
      obs.subscribeOnNext(repairs => {
        const sortedRepairs = Array.from(repairs);
        sortedRepairs.sort((a, b) => a.id - b.id);
        this.setState({repairs: sortedRepairs});
      })
    );
  },

  componentWillUnmount: function() {
    this._repairsSubscription.dispose();
    this._clustersSubscription.dispose();
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    // validate
    const valid = state.currentCluster;
    this.setState({submitEnabled: valid});
    this.props.changeCurrentCluster(this.state.currentCluster);
    console.log("changed cluster to " + this.state.currentCluster);
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

  render: function() {

    const rowsRunning = this.state.repairs.filter(repair => this.state.currentCluster == "all" || this.state.currentCluster == repair.cluster_name).filter(repair => (repair.state == "RUNNING" || repair.state == "PAUSED" || repair.state == "NOT_STARTED")).map(repair =>
      <tbody key={repair.id+'-rows'}>
      <TableRow row={repair} key={repair.id+'-head'}
        deleteSubject={this.props.deleteSubject}
        updateStatusSubject={this.props.updateStatusSubject}/>
      <TableRowDetails row={repair} key={repair.id+'-details'}/>
      </tbody>
    );

    const rowsDone = this.state.repairs.filter(repair => this.state.currentCluster == "all" || this.state.currentCluster == repair.cluster_name).filter(repair => (repair.state != "RUNNING" && repair.state != "PAUSED" && repair.state != "NOT_STARTED")).map(repair =>
      <tbody key={repair.id+'-rows'}>
      <TableRow row={repair} key={repair.id+'-head'}
        deleteSubject={this.props.deleteSubject}
        updateStatusSubject={this.props.updateStatusSubject}/>
      <TableRowDetails row={repair} key={repair.id+'-details'}/>
      </tbody>
    );

    const clusterItems = this.state.clusterNames.map(name =>
      <option key={name} value={name}>{name}</option>
    );

    const clusterFilter = <form className="form-horizontal form-condensed">
            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Filter cluster :</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_currentCluster"
                  onChange={this._handleChange} value={this.state.currentCluster}>
                  <option key="all" value="all">All</option>
                  {clusterItems}
                </select>
              </div>
            </div>
    </form>

    let tableRunning = null;
    if(rowsRunning.length == 0) {
      tableRunning = <div className="alert alert-info" role="alert">No running repair runs found</div>
    } else {

      tableRunning = <div className="row">
          <div className="col-sm-12">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>ID</th>
                              <th>State</th>
                              <th>Cluster</th>
                              <th>Keyspace</th>
                              <th>CFs</th>
                              <th>Incremental</th>
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
    if(rowsDone.length == 0) {
      tableDone = <div className="alert alert-info" role="alert">No past repair runs found</div>
    } else {

      tableDone = <div className="row">
          <div className="col-sm-12">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>ID</th>
                              <th>State</th>
                              <th>Cluster</th>
                              <th>Keyspace</th>
                              <th>CFs</th>
                              <th>Incremental</th>
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

    const runningHeader = <div className="panel-title"><a href="#repairs-running" data-toggle="collapse" onClick={this._toggleRunningDisplay}>Running</a>&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuRunningDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuRunningUpStyle}></span></div>
    const doneHeader = <div className="panel-title"><a href="#repairs-done" data-toggle="collapse" onClick={this._toggleDoneDisplay}>Done</a>&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDoneDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuDoneUpStyle}></span></div>



    return (
            <div>
              {clusterFilter}
              <div className="panel panel-primary">
                <div className="panel-heading">
                  {runningHeader}
                </div>
                <div className="panel-body collapse in" id="repairs-running">
                  {tableRunning}
                </div>
              </div>
              <div className="panel panel-success">
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
