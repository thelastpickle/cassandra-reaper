import React from "react";
import { selectClusterSubject, clusterNames, getClusterStatusSubject } from "observable";

const clusterSelection = React.createClass({

  propTypes: {
  },

  getInitialState: function() {
    return {
      clusterNames: [], currentCluster: null
    };
  },

  componentWillMount: function() {
    let currentCluster: string = $.urlParam('currentCluster');
    if(!currentCluster) {
      currentCluster = 'all';
    }
    this.setState({currentCluster: currentCluster});

    this._clusterNamesSubscription = clusterNames.subscribeOnNext(obs => {
      obs.subscribeOnNext(names => {
        let previousNames = this.state.clusterNames;
        this.setState({clusterNames: names});
        // pre-select only cluster element
        if(names.length == 1) {
          this.setState({currentCluster: names[0]});
          if(previousNames.length == 0) {
            selectClusterSubject.onNext(names[0]);
          }
        }
      });
    });
  },

  componentWillUnmount: function() {
      this._clusterNamesSubscription.dispose();
  },

  _handleClusterChange: function(e) {
    var v = e.target.value;
    selectClusterSubject.onNext(v);
  },

  render: function() {

    const clusterItems = this.state.clusterNames.sort().map(name =>
      <option key={name} value={name}>{name}</option>
    );

    const clusterFilter = <form className="form-horizontal form-condensed">
            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Filter cluster :</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_currentCluster"
                  onChange={this._handleClusterChange} value={this.state.currentCluster}>
                  {clusterItems}
                </select>
              </div>
            </div>
    </form>

    return clusterFilter;
  }

});

export default clusterSelection;