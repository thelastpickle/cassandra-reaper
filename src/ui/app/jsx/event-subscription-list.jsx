import React from "react";
import moment from "moment";
import {RowDeleteMixin, DeleteStatusMessageMixin} from "jsx/mixin";

const TableRow = React.createClass({
  mixins: [RowDeleteMixin],

  propTypes: {
    deleteSubject: React.PropTypes.object.isRequired,
    listenSubscriptionSubject: React.PropTypes.object.isRequired
  },

  _onView: function() {
    this.props.listenSubscriptionSubject.onNext(this.props.row.id);
  },

  render: function() {

    var clExportSse = "fa fa-ban";
    if(this.props.row.export_sse) {
      clExportSse = "fa fa-check-circle";
    }

    var clExportLogger = "fa fa-ban";
    if(this.props.row.export_file_logger) {
      clExportLogger = "fa fa-check-circle";
    }

    var clExportHttp = "fa fa-ban";
    if(this.props.row.export_http_endpoint) {
      clExportHttp = "fa fa-check-circle";
    }

    var nodes = "";
    if(this.props.row.include_nodes) {
      nodes = this.props.row.include_nodes.join(", ");
    }

    var events = "";
    if(this.props.row.events) {
      events = this.props.row.events.join(", ");
    }

    return (
    <tr>
        <td>{this.props.row.description}</td>
        <td>{nodes}</td>
        <td>{events}</td>
        <td><span className={clExportSse}> </span></td>
        <td><span className={clExportLogger}> </span></td>
        <td><span className={clExportHttp}> </span></td>
        <td>
          <button type="button" className="btn btn-xs btn-success" onClick={this._onView}>View</button>
          {this.deleteButton()}
        </td>
    </tr>
    );
  }
});

const eventSubscriptionList = React.createClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    eventSubscriptions: React.PropTypes.object.isRequired,
    deleteSubscriptionSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    listenSubscriptionSubject: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {eventSubscriptions: [], deleteResultMsg: null};
  },

  componentWillMount: function() {
    this._subscriptionsSubscription = this.props.eventSubscriptions.subscribeOnNext(obs =>
      obs.subscribeOnNext(subscriptions => {
        this.setState({eventSubscriptions: subscriptions});
      })
    );
  },

  componentWillUnmount: function() {
    this._subscriptionsSubscription.dispose();
  },

  render: function() {

    function compareNextActivationTime(a,b) {
      if (a.next_activation < b.next_activation)
        return -1;
      if (a.next_activation > b.next_activation)
        return 1;
      return 0;
    }


    const rows = this.state.eventSubscriptions.map(sub =>
      <tbody key={sub.id+'-rows'}>
        <TableRow row={sub} key={sub.id+'-head'}
          deleteSubject={this.props.deleteSubscriptionSubject}
          listenSubscriptionSubject={this.props.listenSubscriptionSubject}/>
      </tbody>
    );

    let table = null;
    if(rows.length == 0) {
      table = <div className="alert alert-info" role="alert">No subscriptions found</div>
    } else {

      table = <div className="row">
          <div className="col-sm-12">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>Description</th>
                              <th>Nodes</th>
                              <th>Events</th>
                              <th>Live View</th>
                              <th>File Logger</th>
                              <th>HTTP Endpoint</th>
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

export default eventSubscriptionList;
