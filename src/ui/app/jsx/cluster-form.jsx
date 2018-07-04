import React from "react";


const clusterForm = React.createClass({

  propTypes: {
    addClusterSubject: React.PropTypes.object.isRequired,
    addClusterResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {addClusterResultMsg: null, seed_node:""};
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

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix
    
    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);
  }, 

  _onAdd: function(e) {
    this.props.addClusterSubject.onNext(this.state.seed_node);
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
              <input type="text" className="form-control" ref="in_seed_node" id="in_seed_node" 
              onChange={this._handleChange}
              placeholder="hostname or ip"></input>
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
