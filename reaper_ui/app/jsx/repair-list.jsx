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
    deleteSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    updateStatusSubject: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {repairs: [], deleteResultMsg: null};
  },

  componentWillMount: function() {
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
  },

  render: function() {

    const rows = this.state.repairs.map(repair =>
      <tbody key={repair.id+'-rows'}>
      <TableRow row={repair} key={repair.id+'-head'}
        deleteSubject={this.props.deleteSubject}
        updateStatusSubject={this.props.updateStatusSubject}/>
      <TableRowDetails row={repair} key={repair.id+'-details'}/>
      </tbody>
    );

    let table = null;
    if(rows.length == 0) {
      table = <div className="alert alert-info" role="alert">No repair runs found</div>
    } else {

      table = <div className="row">
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
                        {rows}
                  </table>
              </div>
          </div>
      </div>;
    }

    return (<div className="panel panel-default">
              <div className="panel-heading">
                <div className="panel-title">Done</div>
              </div>
              <div className="panel-body">
                {this.deleteMessage()}
                {table}
              </div>
            </div>);
  }
});

export default repairList;
