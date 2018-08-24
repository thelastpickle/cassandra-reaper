//
//  Copyright 2015-2016 Stefan Podkowinski
//  Copyright 2016-2018 The Last Pickle Ltd
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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

