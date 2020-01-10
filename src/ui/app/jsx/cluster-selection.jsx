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
import Select from 'react-select';
import { selectClusterSubject, clusterSelected, clusterNames } from "observable";

const clusterSelection = CreateReactClass({

  propTypes: {
  },

  getInitialState: function() {
    return {
      clusterNames: [],
      currentCluster: "",
    };
  },

  UNSAFE_componentWillMount: function() {
    const currentCluster: string = $.urlParam('currentCluster');

    if (currentCluster !== "null") {
      this.setState({currentCluster: currentCluster});
    }

    this._clusterNamesSubscription = clusterNames.subscribeOnNext(obs => {
      obs.subscribeOnNext(names => {
        if (!this.state.currentCluster) {
          // pre-select cluster
          if (names.length) {
            this.setState({currentCluster: names[0]});
            selectClusterSubject.onNext(names[0]);
          }
        }
        this.setState({clusterNames: names});
      });
    });

    this._clusterSelectedSubscription = clusterSelected.subscribeOnNext(name => {
      this.setState({currentCluster: name});
    });
  },

  componentWillUnmount: function() {
      this._clusterNamesSubscription.dispose();
      this._clusterSelectedSubscription.dispose();
  },

  _handleSelectOnChange: function(valueContext, actionContext) {
      selectClusterSubject.onNext(valueContext.value);
  },

  render: function() {
    const selectClusterItems = this.state.clusterNames.sort().map(name => {
      { return { value: name, label: name}; }
    });

    const clusterFilter = <form className="form-horizontal form-condensed">
            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Filter cluster :</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <Select
                  id="in_clusterName"
                  name="in_clusterName"
                  classNamePrefix="select"
                  options={selectClusterItems}
                  onChange={this._handleSelectOnChange}
                  value={{ value: this.state.currentCluster, label: this.state.currentCluster}}
                  placeholder="Select a cluster"
                />
              </div>
            </div>
    </form>

    return clusterFilter;
  }

});

export default clusterSelection;
