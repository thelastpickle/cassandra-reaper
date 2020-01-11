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
import CreateReactClass from 'create-react-class';
import PropTypes from 'prop-types';


const clusterForm = CreateReactClass({

  propTypes: {
    addClusterSubject: PropTypes.object.isRequired,
    addClusterResult: PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {addClusterResultMsg: null, seed_node:"", jmx_port:"7199", submitEnabled: false};
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

    // validate
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.seed_node.length > 0;
    this.setState({submitEnabled: valid});
  },

  _toggleFormDisplay: function() {
    if(this.state.formCollapsed == true) {
      this.setState({formCollapsed: false});
    }
    else {
      this.setState({formCollapsed: true});
    }
  },

  _onAdd: function(e) {
    let addClusterObj = {
        'seed_node':this.state.seed_node,
        'jmx_port':this.state.jmx_port,
        'jmx_username':this.state.jmx_username,
        'jmx_password':this.state.jmx_password};
    this.props.addClusterSubject.onNext(addClusterObj);
  },

  render: function() {

    let addMsg = null;
    if(this.state.addClusterResultMsg) {
      addMsg = <div className="alert alert-danger" role="alert">{this.state.addClusterResultMsg}</div>
    }

     const form = <div className="row">
         <div className="col-lg-12">

           <form className="form-horizontal form-condensed">

             <div className="form-group">
               <label htmlFor="in_seed_node" className="col-sm-3 control-label">Seed node*</label>
               <div className="col-sm-9 col-md-7 col-lg-5">
                 <input type="text" className="form-control" ref="in_seed_node" id="in_seed_node"
                     onChange={this._handleChange}
                     placeholder="hostname or ip"></input>
               </div>
             </div>

             <div className="form-group">
               <label htmlFor="in_jmx_port" className="col-sm-3 control-label">JMX port</label>
               <div className="col-sm-9 col-md-7 col-lg-5">
                 <input type="text" className="form-control" ref="in_jmx_port" id="in_jmx_port"
                     onChange={this._handleChange}
                     placeholder="7199"></input>
               </div>
             </div>

             <div className="form-group">
               <label htmlFor="in_jmx_username" className="col-sm-3 control-label">JMX username</label>
               <div className="col-sm-9 col-md-7 col-lg-5">
                 <input type="text" className="form-control" ref="in_jmx_username" id="in_jmx_username"
                   onChange={this._handleChange}
                   placeholder="JMX username of cluster (optional)"></input>
               </div>
             </div>

             <div className="form-group">
               <label htmlFor="in_jmx_username" className="col-sm-3 control-label">JMX password</label>
               <div className="col-sm-9 col-md-7 col-lg-5">
                 <input type="password" className="form-control" ref="in_jmx_password" id="in_jmx_password"
                   onChange={this._handleChange}
                   placeholder="JMX password of cluster (optional)"></input>
               </div>
             </div>
             <div className="form-group">
               <div className="col-sm-offset-3 col-sm-9">
                 <button type="button" className="btn btn-warning" disabled={!this.state.submitEnabled}
                   onClick={this._onAdd}>Add Cluster</button>
               </div>
             </div>
           </form>

       </div>
     </div>

    let menuDownStyle = {
      display: "inline-block"
    }

    let menuUpStyle = {
      display: "none"
    }

    if(this.state.formCollapsed == false) {
      menuDownStyle = {
        display: "none"
      }
      menuUpStyle = {
        display: "inline-block"
      }
    }

    const formHeader = <div className="panel-title">
                          <a href="#cluster-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>Add Cluster
                          &nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span>
                                 <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span></a></div>

    return (<div className="panel panel-warning">
              <div className="panel-heading">
                {formHeader}
              </div>
              <div className="panel-body collapse" id="cluster-form">
                {addMsg}
                {form}
              </div>
            </div>);
  }
});

export default clusterForm;
