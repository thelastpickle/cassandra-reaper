import React from "react";

var serverStatus = React.createClass({

  propTypes: {
    statusObservableTimer: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {isDisconnected: false};
  },

  componentWillMount: function() {
    this._statusSubscription = this.props.statusObservableTimer.subscribeOnNext(obs =>
        obs.subscribe(this._onStatusOk, this._onStatusError, this._onStatusOk));
  },

  componentWillUnmount: function() {
    this._statusSubscription.dispose();
  },

  _onStatusOk: function(data, e, f, g) {
    this.setState({isDisconnected: false});
  },

  _onStatusError: function(e) {
    this.setState({isDisconnected: true});
  },

  render: function() {

    if(this.state.isDisconnected) {
      return <div className="alert alert-danger" role="alert">Server not available! Could not contact reaper server.</div>
    } else {
      return <div/>
    }
  }
});

export default serverStatus;

