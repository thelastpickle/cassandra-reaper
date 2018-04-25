import React from "react";
import _multiselect from "bootstrap-multiselect.js";

const eventScreen = React.createClass({

  propTypes: {
    onChange: React.PropTypes.func.isRequired,
    clusterStatusResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {
      applied: false,
      endpointsByCluster: {}
    };
  },

  componentWillMount: function() {
    this._clusterStatusSubscription = this.props.clusterStatusResult.subscribeOnNext(obs => {
      obs.subscribeOnNext(state => {
        if(!state.nodes_status) {
          console.error("Missing nodes_status result");
          return;
        }
        const dcs = state.nodes_status.endpointStates.map(epState => Object.keys(epState.endpoints));

        const epCurrent = {};
        const epStates = state.nodes_status.endpointStates;
        if(epStates.length == 0) {
          console.error("No endpoint states found");
          return;
        }

        const eps = epStates[0].endpoints;
        for(let dc in eps) {
          epCurrent[dc] = Object.values(eps[dc]).map(epsByRack => epsByRack.map(ep => ep.endpoint))
            .reduce(Function.prototype.apply.bind(Array.prototype.concat)); // flatten racks
        }
        this.replaceState({endpointsByCluster: epCurrent});
      });
    });
  },

  componentWillUnmount: function() {
    this._clusterStatusSubscription.dispose();
  },

  _handleChange: function(e) {
    let v = e.target.value;
    let n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);
  },

  componentDidUpdate: function(prevProps, prevState) {
    if(prevState.applied) return;

    const el = $('#node-selection');
    if(!el) return;

    el.multiselect({
        //enableFiltering: true,
        enableClickableOptGroups: true,
        onChange: this.props.onChange
    });

    this.setState({applied: true});
  },

  render: function() {

    if(Object.keys(this.state.endpointsByCluster).length == 0) {
      return <div className="row"/>;
    }

    const optByDc = {};
    for(let dc in this.state.endpointsByCluster) {
      const epsByCluster = this.state.endpointsByCluster[dc];
      optByDc[dc] = epsByCluster.map(ep => <option key={dc + "_" + ep} value={ep}>{ep}</option>);
    }

    const optGroups = Object.keys(this.state.endpointsByCluster).map(dc => <optgroup key={dc} label={dc}>{optByDc[dc]}</optgroup>);

    return (
      <select id="node-selection" multiple="multiple">
        {optGroups}
      </select>
    );
  }

});

export default eventScreen;