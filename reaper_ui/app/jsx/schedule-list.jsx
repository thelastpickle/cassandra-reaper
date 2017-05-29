import React from "react";
import moment from "moment";
import {RowDeleteMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const TableRow = React.createClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin],

  render: function() {

    const next = moment(this.props.row.next_activation).fromNow();
    const rowID = `#details_${this.props.row.id}`;
    const incremental = this.props.row.incremental_repair == true ? "true" : "false";

    return (
    <tr>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.id}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.state}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.cluster_name}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.keyspace_name}</td>
        <td data-toggle="collapse" data-target={rowID}>{incremental}</td>
        <td data-toggle="collapse" data-target={rowID}>{next}</td>
        <td data-toggle="collapse" data-target={rowID}>{this.props.row.scheduled_days_between} days</td>
        <td>
          {this.statusUpdateButton()}
          {this.deleteButton()}
        </td>
    </tr>
    );
  }
});


const TableRowDetails = React.createClass({
  render: function() {

    const createdAt = moment(this.props.row.creation_time).format("LLL");
    const nextAt = moment(this.props.row.next_activation).format("LLL");
    const rowID = `details_${this.props.row.id}`;
    const incremental = this.props.row.incremental_repair == true ? "true" : "false";

    return (
      <tr id={rowID} className="collapse out">
        <td colSpan="7">
          <table className="table table-condensed">
            <tbody>
                <tr>
                    <td>Next run</td>
                    <td>{nextAt}</td>
                </tr>
                <tr>
                    <td>Owner</td>
                    <td>{this.props.row.owner}</td>
                </tr>
                <tr>
                    <td>CFs</td>
                    <td><CFsListRender list={this.props.row.column_families} /></td>
                </tr>
                <tr>
                    <td>Incremental</td>
                    <td>{incremental}</td>
                </tr>
                <tr>
                    <td>Segment count</td>
                    <td>{this.props.row.segment_count}</td>
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


const scheduleList = React.createClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    schedules: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    updateStatusSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {schedules: [], deleteResultMsg: null};
  },

  componentWillMount: function() {
    this._schedulesSubscription = this.props.schedules.subscribeOnNext(obs =>
      obs.subscribeOnNext(schedules => {
        const sortedSchedules = Array.from(schedules);
        sortedSchedules.sort((a, b) => a.id - b.id);
        this.setState({schedules: sortedSchedules});
      })
    );
  },

  componentWillUnmount: function() {
    this._schedulesSubscription.dispose();
  },

  render: function() {

    const rows = this.state.schedules.map(schedule =>
      <tbody key={schedule.id+'-rows'}>
        <TableRow row={schedule} key={schedule.id+'-head'}
          deleteSubject={this.props.deleteSubject}
          updateStatusSubject={this.props.updateStatusSubject}/>
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
                              <th>ID</th>
                              <th>State</th>
                              <th>Cluster</th>
                              <th>Keyspace</th>
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
                {this.deleteMessage()}
                {table}
              </div>
            </div>);
  }
});

export default scheduleList;
