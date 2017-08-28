import React from "react";


const clusterForm = React.createClass({

  propTypes: {
    addClusterSubject: React.PropTypes.object.isRequired,
    addClusterResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {addClusterResultMsg: null};
  },

  componentWillMount: function() {
    this._clusterResultSubscription = this.props.addClusterResult.subscribeOnNext(obs =>
      obs.subscribe(
        r => this.setState({addClusterResultMsg: null}),
        r => this.setState({addClusterResultMsg: r.responseText})
      )
    );
  },

  componentWillUnmount: function() {
    this._clusterResultSubscription.dispose();
  },

  _onAdd: function(e) {
    const seed = React.findDOMNode(this.refs.in_seed_node).value;
    this.props.addClusterSubject.onNext(seed);
  },

  render: function() {

    let addMsg = null;
    if(this.state.addClusterResultMsg) {
      addMsg = <div className="alert alert-danger" role="alert">{this.state.addClusterResultMsg}</div>
    }

    const form = <div className="row">
        <div className="col-lg-12">

          <div className="form-inline">
            <div className="form-group">
              <label htmlFor="in_seed_node">Seed node:</label>
              <input type="text" className="form-control" ref="in_seed_node" id="in_seed_node" placeholder="hostname or ip"></input>
            </div>
            <button type="button" className="btn btn-success" onClick={this._onAdd}>Add Cluster</button>
          </div>

      </div>
    </div>


    return (<div className="panel panel-default">
              <div className="panel-body">
                {addMsg}
                {form}
              </div>
            </div>);
  }
});

export default clusterForm;
