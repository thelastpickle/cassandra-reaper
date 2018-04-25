import React from "react";
import clusterFilterSelection from "observable";

const diagnosticEvents = new Rx.Subject();

const ClusterFilterSelection = React.createClass({

  propTypes: {
    clusterNames: React.PropTypes.object.isRequired,
    currentCluster: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {currentCluster: this.props.currentCluster};
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    // notify
    clusterFilterSelection.onNext(state.currentCluster);
//    const valid = state.currentCluster;
//    this.setState({submitEnabled: valid});
//    this.props.changeCurrentCluster(this.state.currentCluster);
  },

  render: function() {

    const clusterItems = this.state.clusterNames.map(name =>
      <option key={name} value={name}>{name}</option>
    );

    return <form className="form-horizontal form-condensed">
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

  }
});

export default ClusterFilterSelection;