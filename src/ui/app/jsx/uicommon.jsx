//
//  Copyright 2018-2018 Stefan Podkowinski
//  Copyright 2019-2019 The Last Pickle Ltd
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
import CreateReactClass from 'create-react-class';
import PropTypes from 'prop-types';
import clusterFilterSelection from "observable";

const diagnosticEvents = new Rx.Subject();

const ClusterFilterSelection = CreateReactClass({

  propTypes: {
    clusterNames: PropTypes.object.isRequired,
    currentCluster: PropTypes.object.isRequired
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
