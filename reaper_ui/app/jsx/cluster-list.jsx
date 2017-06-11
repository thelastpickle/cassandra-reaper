import React from "react";
import {DeleteStatusMessageMixin} from "jsx/mixin";


const TableRow = React.createClass({
  render: function() {
    return (
    <tr>
        <td><a href={'repair.html?currentCluster=' + this.props.name}> {this.props.name}</a></td>
        <td>
          <button type="button" className="btn btn-xs btn-danger" onClick={this._onDelete}>Delete</button>
        </td>
    </tr>
    );
  },

  _onDelete: function(e) {
    this.props.deleteSubject.onNext(this.props.name);
  }

});

const clusterList = React.createClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {clusterNames: [], deleteResultMsg: null};
  },

  componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );
  },

  componentWillUnmount: function() {
    this._clusterNamesSubscription.dispose();
  },

  render: function() {

    const rows = this.state.clusterNames.map(name =>
      <TableRow name={name} key={name} deleteSubject={this.props.deleteSubject}/>);

    let table = null;
    if(rows.length == 0) {
      table = <div className="alert alert-info" role="alert">No clusters found</div>
    } else {

      table = <div className="row">
          <div className="col-lg-6">
              <div className="table-responsive">
                  <table className="table table-bordered table-hover table-striped">
                      <thead>
                          <tr>
                              <th>Cluster Name</th>
                              <th></th>
                          </tr>
                      </thead>
                      <tbody>
                        {rows}
                      </tbody>
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

export default clusterList;