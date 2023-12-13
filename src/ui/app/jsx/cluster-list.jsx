//
//  Copyright 2015-2016 Stefan Podkowinski
//  Copyright 2016-2019 The Last Pickle Ltd
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
import NodeStatus from "jsx/node-status";
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix} from "jsx/mixin";
import Modal from 'react-bootstrap/lib/Modal';
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import Badge from 'react-bootstrap/lib/Badge';
import $ from "jquery";
var NotificationSystem = require('react-notification-system');

const Cluster = CreateReactClass({

  propTypes: {
    name: PropTypes.string.isRequired,
    clusterFilter: PropTypes.string.isRequired,
    nodeFilter: PropTypes.string.isRequired
  },
  
  getInitialState: function() {
    const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
    return {clusterStatus: {}, 
            clusterStatuses: null, 
            urlPrefix: URL_PREFIX , 
            nbNodes: 0, 
            nodesDown:0,
            refreshing: true,
            nodes_status: null
          };
  },

  UNSAFE_componentWillMount: function() {
    this._refreshClusterStatus();
  },

  _refreshClusterStatus: function() {
    $.ajax({
          url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(this.props.name),
          method: 'GET',
          component: this,
          complete: function(data) {
            this.component.setState({clusterStatuses: setTimeout(this.component._refreshClusterStatus, 30000),
                                     clusterStatus: $.parseJSON(data.responseText)});
            
            if(this.component.state.clusterStatus.nodes_status){
              this.component.setState({nodes_status: this.component.state.clusterStatus.nodes_status});
            }
          },
          error: function(data) {
            console.error(this.component.props.name + " failed.");
          }
      });
  },

  componentWillUnmount: function() {
    clearInterval(this.state.clusterStatuses);
  },

  chunkArray: function(myArray, chunk_size){
    var index = 0;
    var arrayLength = myArray.length;
    var tempArray = [];
    
    for (index = 0; index < arrayLength; index += chunk_size) {
        var myChunk = myArray.slice(index, index+chunk_size);
        // Do something if you want with the group
        tempArray.push(myChunk);
    }

    return tempArray;
  },

  render: function() {

    let rowDivStyle = {
      marginLeft: "0",
      marginRight: "0"
    };

    let progressStyle = {
      marginTop: "0.25em",
      marginBottom: "0.25em"
    };

    let datacenters = "";
    let runningRepairs = 0;

    let repairProgress = "";
    let totalLoad = 0;

    let jmxPasswordString = this.state.clusterStatus.jmx_password_is_set ? "*******" : "";

    if (this.state.clusterStatus.repair_runs) {
      runningRepairs = this.state.clusterStatus.repair_runs.reduce(function(previousValue, repairRun){
                              return previousValue + (repairRun.state === 'RUNNING' ? 1: 0);
                            }, 0);

      repairProgress = this.state.clusterStatus.repair_runs.filter(repairRun => repairRun.state === 'RUNNING').map(repairRun =>
                      <ProgressBar now={(repairRun.segments_repaired*100)/repairRun.total_segments} active bsStyle="success" 
                                   style={progressStyle} 
                                   label={repairRun.keyspace_name}
                                   key={repairRun.id}/>
      );
    }
    
    if(this.state.nodes_status != null) {
        var grouped_datacenters = this.chunkArray(Object.keys(this.state.nodes_status.endpointStates[0].endpoints).sort(), 3);
        datacenters = grouped_datacenters.map(dcGroup => 
          <div className="row" key={dcGroup[0]}>
            {dcGroup.map(dc => 
                        <Datacenter datacenter={this.state.nodes_status.endpointStates[0].endpoints[dc]} 
                                    datacenterName={dc} 
                                    key={this.props.name + "-" + dc}
                                    nbDatacenters={Object.keys(this.state.nodes_status.endpointStates[0].endpoints).length} 
                                    clusterName={this.props.name} key={this.props.name + '-' + dc} 
                                    totalLoad={this.state.nodes_status.endpointStates[0].totalLoad}
                                    nodeFilter={this.props.nodeFilter}/>)
            }
          </div>
       );
       totalLoad = this.state.nodes_status.endpointStates[0].totalLoad;
      } 
      else {
        datacenters = <div className="clusterLoader"></div>;
      }

    let runningRepairsBadge = <span className="label label-default">{runningRepairs}</span>;
    if(runningRepairs > 0) {
      runningRepairsBadge = <span className="label label-success">{runningRepairs}</span>;
    }

    let clusterDisplayStyle = {
      display: "none" 
    };

    if(this.props.name.includes(this.props.clusterFilter)) {
      clusterDisplayStyle = {
        display: "block"
      }
    }

    var nbNodes = 0;
    if(this.state.nodes_status != null) {
      nbNodes = this.state.nodes_status.endpointStates[0].endpointNames.length;
    }

    return (
      <div className="panel panel-default" style={clusterDisplayStyle}>
        <div className="panel-body">
          <div className="row">
            <div className="col-lg-12">
              <a className="btn btn-lg btn-default" href={'repair.html?currentCluster=' + this.props.name}><h4>{this.props.name}</h4></a><br/><br/>  
            </div>
          </div>
          
          <div className="row">
            <div className="col-lg-2">
              <div className="font-bold">Nodes: <span className="badge">{nbNodes}</span></div>
              <div className="font-bold">Total load: <span className="badge">{humanFileSize(totalLoad,1024)}</span></div>
              <div className="font-bold">Running repairs: {runningRepairsBadge}</div>
              <button type="button" className="forget-cluster-button btn btn-xs btn-danger" onClick={this._onDelete}>Forget cluster</button>
              <button
                type="button"
                className="cluster-info-button btn btn-xs btn-info"
                data-toggle="modal"
                data-target="#clusterInfoModal">Info</button>
            </div>
            <div className="col-lg-10">
              <div className="row" style={rowDivStyle}>
                {datacenters}
              </div>
              <div className="repair-progress-bar">
                {repairProgress}
              </div>
            </div>
          </div>
        </div>

        <div className="modal fade" id="clusterInfoModal">
          <Modal.Dialog>
            <Modal.Header>
              <Modal.Title><span className="text-center"><h2>{this.props.name} Information</h2></span></Modal.Title>
              <button type="button" className="close" data-dismiss="modal">×</button>
            </Modal.Header>
            <Modal.Body>
              <p><span className="font-bold">JMX username:</span> {this.state.clusterStatus.jmx_username}</p>
              <p><span className="font-bold">JMX password:</span> {jmxPasswordString}</p>
            </Modal.Body>
          </Modal.Dialog>
        </div>
      </div>
    );
  },

  _onDelete: function(e) {
    this.props.deleteSubject.onNext(this.props.name);
  }
});


const Datacenter = CreateReactClass({

  propTypes: {
    datacenter: PropTypes.object.isRequired,
    datacenterName: PropTypes.string.isRequired,
    nbDatacenters: PropTypes.number.isRequired,
    clusterName: PropTypes.string.isRequired,
    totalLoad: PropTypes.number.isRequired,
    nodeFilter: PropTypes.string.isRequired
  },
  
  render: function() {
    const dcSize = Object.keys(this.props.datacenter).map(rack => this.props.datacenter[rack].reduce(function(previousValue, endpoint){
                              return previousValue + endpoint.load; 
                            }, 0)).reduce(function(previousValue, currentValue){
                              return previousValue + currentValue; 
                            }, 0);
    let rowDivStyle = {
      marginLeft: "0",
      paddingLeft: "0",
      paddingRight: "1px",
      width: ((dcSize/this.props.totalLoad)*100) + "%"
    };

    let badgeStyle = {
      float: "right"
    };

    let panelHeadingStyle = {
      padding: "2px 10px"
    };

    let panelBodyStyle = {
      padding: "1px"
    };

    let panelStyle = {
      marginBottom: "1px"
    };

    const nbRacks = Object.keys(this.props.datacenter).length;
    const racks = Object.keys(this.props.datacenter).sort().map(rack => 
          <Rack key={this.props.datacenterName+'-'+rack} rack={this.props.datacenter[rack]}
                nbRacks={nbRacks} clusterName={this.props.clusterName} dcLoad={dcSize}
                totalLoad={this.props.totalLoad} nodeFilter={this.props.nodeFilter}/>);
    return (
            <div className="col-lg-12" style={rowDivStyle}>
              <div className="panel panel-info" style={panelStyle}>
                <div className="panel-heading" style={panelHeadingStyle}><b>{this.props.datacenterName} <Badge>{humanFileSize(dcSize, 1024)}</Badge></b></div>
                <div className="panel-body" style={panelBodyStyle}>{racks}</div>
              </div>
            </div>
    );
  },
});

const Rack = CreateReactClass({
  _notificationSystem: null,

  propTypes: {
    rack: PropTypes.array.isRequired,
    nbRacks: PropTypes.number.isRequired,
    clusterName: PropTypes.string.isRequired,
    dcLoad: PropTypes.number.isRequired,
    totalLoad: PropTypes.number.isRequired,
    nodeFilter: PropTypes.string.isRequired
  },

  componentDidMount: function() {
    this._notificationSystem = this.refs.notificationSystem;
  },

  render: function() {
    const rackSize = this.props.rack.reduce((previousValue, endpoint) => previousValue + endpoint.load, 0);
    var rackWidth = 100;
    if (this.props.rack.length < 3) {
      rackWidth = ((rackSize/this.props.dcLoad)*100)
    }
    let rowDivStyle = {
        marginLeft: "0",
        paddingLeft: "0",
        paddingRight: "1px",
        width: rackWidth + "%"
    };

    let badgeStyle = {
      float: "right"
    };

    let panelHeadingStyle = {
      padding: "2px 10px"
    };

    let panelBodyStyle = {
      padding: "0px"
    };

    let panelStyle = {
      marginBottom: "1px"
    };

    let nodes = "" ;
    let rackName = "";

    if(this.props.rack) {
      rackName = this.props.rack[0].rack;
      nodes = this.props.rack.map(endpoint =>
          <NodeStatus key={endpoint.endpoint} endpointStatus={endpoint}
            clusterName={this.props.clusterName} nbNodes={this.props.rack.length}
            rackLoad={rackSize} notificationSystem={this._notificationSystem} totalLoad={this.props.totalLoad}
            nodeFilter={this.props.nodeFilter}/>
      );
    }

    return (
      <div className="col-lg-12" style={rowDivStyle}>
        <NotificationSystem ref="notificationSystem" />
        <div className="panel panel-success" style={panelStyle}>
          <div className="panel-heading" style={panelHeadingStyle}><b>{rackName} <Badge>{humanFileSize(rackSize, 1024)}</Badge></b></div>
          <div className="panel-body" style={panelBodyStyle}>{nodes}</div>
        </div>
      </div>
    );
  },
});

const clusterList = CreateReactClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    clusterNames: PropTypes.object.isRequired,
    deleteSubject: PropTypes.object.isRequired,
    deleteResult: PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {clusterNames: [], deleteResultMsg: null, clusterFilter: "", nodeFilter: ""};
  },

  UNSAFE_componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );
  },

  componentWillUnmount: function() {
    this._clusterNamesSubscription.dispose();
  },

  _handleChange: function(e) {
    const v = e.target.value;
    const n = e.target.id.substring(3); // strip in_ prefix
    
    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);
  }, 

  render: function() {
    const rows = this.state.clusterNames.sort().map(name =>
      <Cluster name={name} key={name} deleteSubject={this.props.deleteSubject} getClusterStatus={this.props.getClusterStatus} getClusterSubject={this.props.getClusterSubject} clusterFilter={this.state.clusterFilter} nodeFilter={this.state.nodeFilter}/>);

    let table = null;
    if(rows.length === 0) {
      table = <div className="alert alert-info" role="alert">No clusters found</div>;
    } else {
        table = <div>{rows}</div>;
    }

    let filterInput = <div className="row">
      <div className="col-lg-12">
        <form className="form-horizontal form-condensed">
          <div className="form-group">
            <label htmlFor="in_clusterName" className="col-sm-3 control-label">Filter: </label>
            <div className="col-sm-2">
              <input type="text" required className="form-control" value={this.state.clusterFilter}
                    onChange={this._handleChange} id="in_clusterFilter" placeholder="Start typing to filter clusters..."/>
            </div>
            <div className="col-sm-2">
              <input type="text" required className="form-control" value={this.state.nodeFilter}
                    onChange={this._handleChange} id="in_nodeFilter" placeholder="Start typing to filter nodes..."/>
            </div>
          </div>
        </form>
      </div>
    </div>;

    return (<div className="row">
              <div className="col-lg-12">
                {this.deleteMessage()}
                {filterInput}
                {table}
              </div>
            </div>);
  }
});

export default clusterList;
