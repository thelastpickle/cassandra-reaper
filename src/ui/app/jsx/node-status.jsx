//
//  Copyright 2018-2018 The Last Pickle Ltd
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
import Snapshot from "jsx/snapshot";
import TpStats from "jsx/tpstats";
import Streams from "jsx/streams";
import DroppedMessages from "jsx/dropped-messages";
import ClientRequestLatency from "jsx/client-request-latency";
import ActiveCompactions from "jsx/active-compactions";
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import Modal from 'react-bootstrap/lib/Modal';
import Button from 'react-bootstrap/lib/Button';
import Tooltip from 'react-bootstrap/lib/Tooltip';
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger';
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import Popover from 'react-bootstrap/lib/Popover';
import Tabs from 'react-bootstrap/lib/Tabs';
import Tab from 'react-bootstrap/lib/Tab';
import $ from "jquery";
var NotificationSystem = require('react-notification-system');

const NodeStatus = CreateReactClass({

    propTypes: {
      endpointStatus: PropTypes.object.isRequired,
      clusterName: PropTypes.string.isRequired,
      nbNodes: PropTypes.number.isRequired,
      rackLoad: PropTypes.number.isRequired,
      notificationSystem: PropTypes.object,
      totalLoad: PropTypes.number.isRequired,
      nodeFilter: PropTypes.string
    },
  
    getInitialState() {
      const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
      return { showModal: false, snapshots: [], urlPrefix: URL_PREFIX, 
              snapshotsSizeOnDisk:{}, snapshotsTrueSize:{},
              totalSnapshotSizeOnDisk: 0, totalSnapshotTrueSize: 0,
              communicating: false, tokens:[]};
    },

    UNSAFE_componentWillMount: function() {
      this._getNodeTokens();
    },
  
    _getNodeTokens: function() {
      $.ajax({
            url: this.state.urlPrefix + '/node/tokens/' + encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpointStatus.endpoint),
            method: 'GET',
            component: this,
            complete: function(data) {
              try {
                this.component.setState({tokens: $.parseJSON(data.responseText)});
              }
              catch(error) {
                console.log("Failed grabbing the tokens...");
              }
            },
            error: function(data) {
              console.log("Failed grabbing the tokens...");
            }
        });
    },
  
   close() {
      this.setState({ showModal: false });
    },
  
    open() {
      this.setState({ showModal: true });
      this._listSnapshots();
    },
  
    _listSnapshots: function() {
      $.ajax({
            url: getUrlPrefix(window.top.location.pathname) + '/snapshot/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpointStatus.endpoint),
            method: 'GET',
            component: this,
            complete: function(data) {
              this.component.setState({snapshotsSizeOnDisk:{}, snapshotsTrueSize:{},
                totalSnapshotSizeOnDisk: 0, totalSnapshotTrueSize: 0});
              this.component.setState({snapshots: $.parseJSON(data.responseText)});
              let snapshotSizeOnDisk = {};
              let snapshotTrueSize = {};
              Object.keys(this.component.state.snapshots).sort().forEach(snapshotName => 
                snapshotSizeOnDisk[snapshotName] = this.component.state.snapshots[snapshotName].map(table => table.sizeOnDisk).reduce((a, b) => a + b), 0);
  
              Object.keys(this.component.state.snapshots).sort().forEach(snapshotName => 
                snapshotTrueSize[snapshotName] = this.component.state.snapshots[snapshotName].map(table => table.trueSize).reduce((a, b) => a + b), 0);
  
              Object.keys(this.component.state.snapshots).forEach(snapshotName => {
                this.component.addSnapshotSizes(snapshotName, snapshotTrueSize[snapshotName], snapshotSizeOnDisk[snapshotName]);
              });
            }
        });
    },
  
    _takeSnapshot: function() {
      this.setState({communicating: true}); 
      toast(this.props.notificationSystem, "Taking a new snapshot...", "warning", this.props.endpointStatus.endpoint);   
      $.ajax({
            url: getUrlPrefix(window.top.location.pathname) + '/snapshot/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpointStatus.endpoint),
            dataType: 'text',
            method: 'POST',
            component: this,
            success: function(data) {
              toast(this.component.props.notificationSystem, "Successfully took a new snapshot", "success", this.component.props.endpointStatus.endpoint); 
            },
            complete: function(data) {
              this.component.setState({communicating: false});
              this.component._listSnapshots();
            },
            error: function(data) {
              toast(this.component.props.notificationSystem, "Failed taking a snapshot on node " + this.component.props.endpoint + " : " + data.responseText, "error", this.component.props.endpoint);
            }
        });
    },
  
    _takeSnapshotClusterWide: function() {
      this.setState({communicating: true}); 
      toast(this.props.notificationSystem, "Taking a new snapshot...", "warning", this.props.endpointStatus.endpoint);   
      $.ajax({
            url: getUrlPrefix(window.top.location.pathname) + '/snapshot/cluster/' + encodeURIComponent(this.props.clusterName),
            dataType: 'text',
            method: 'POST',
            component: this,
            success: function(data) {
                toast(this.component.props.notificationSystem, "Successfully took a new snapshot", "success", this.component.props.clusterName); 
            },
            complete: function(data) {
              this.component.setState({communicating: false});
              this.component._listSnapshots();
            },
            error: function(data) {
                toast(this.component.props.notificationSystem, "Failed taking a snapshot on cluster " + this.component.props.clusterName + " : " + data.responseText, "error", this.component.props.clusterName);
            }
        });
    },
  
    addSnapshotSizes: function(snapshotName, snapshotTrueSize, snapshotSizeOnDisk) {
      let tmpSnapshotsSizeOnDisk = this.state.snapshotsSizeOnDisk;
      tmpSnapshotsSizeOnDisk[snapshotName] = snapshotSizeOnDisk;
      let tmpSnapshotsTrueSize = this.state.snapshotsTrueSize;
      tmpSnapshotsTrueSize[snapshotName] = snapshotTrueSize;
      const totalSizeOnDisk = Object.entries(tmpSnapshotsSizeOnDisk).map(entry => entry[1]).reduce((a, b) => a + b, 0);
      const totalTrueSize = Object.entries(tmpSnapshotsTrueSize).map(entry => entry[1]).reduce((a, b) => a + b, 0);
      this.setState({snapshotsSizeOnDisk: tmpSnapshotsSizeOnDisk, snapshotsTrueSize: tmpSnapshotsTrueSize,
                     totalSnapshotSizeOnDisk: totalSizeOnDisk, totalSnapshotTrueSize: totalTrueSize});
  
    },
  
    render: function() {
      let displayNodeStyle = {
        display: "none"
      }

      if (this.props.nodeFilter == "" || this.props.endpointStatus.endpoint.includes(this.props.nodeFilter)) {
        displayNodeStyle = {
        }
      }
      
      let progressStyle = {
        display: "none" 
      }
  
      let takeSnapshotStyle = {
        display: "inline-block" 
      }

      if (this.state.communicating==true) {
        progressStyle = {
          display: "inline-block"
        }
        takeSnapshotStyle = {
          display: "none"
        } 
      }
  
      let buttonStyle = "btn btn-xs btn-success";
      let largeButtonStyle = "btn btn-lg btn-success";
  
      if(!this.props.endpointStatus.status.endsWith('UP')){
        buttonStyle = "btn btn-xs btn-danger";
        largeButtonStyle = "btn btn-lg btn-danger";
      }
      
      let averageSize = this.props.rackLoad/this.props.nbNodes;
      var opacity = Math.max(0.3, (0.7*(this.props.endpointStatus.load/averageSize)));
      const btStyle = {
        width: "22px",
        height: "22px",
        margin:"1px",
        opacity: opacity,
        borderRadius: "4px",
        border: "1px solid #a5d2a5"
      };

      const overflowStyle = {
        overflow: "auto",
        height: "200px"
      }
  
      const tooltip = (
        <Tooltip id="tooltip"><strong>{this.props.endpointStatus.endpoint}</strong> ({humanFileSize(this.props.endpointStatus.load, 1024)})</Tooltip>
      );

      const tokenList = this.state.tokens.map(token => <div key={token}>{token}</div>);

      const tokens = (
        <Popover id="tokens" title="Tokens" trigger="click">
          <div style={overflowStyle}>{tokenList}</div>
        </Popover>
      );
  
      const snapshots = Object.keys(this.state.snapshots).sort().map(snapshotName => 
        <Snapshot snapshotName={snapshotName} snapshots={this.state.snapshots[snapshotName]}
                  totalSizeOnDisk={this.state.snapshotsSizeOnDisk[snapshotName]}
                  totalTrueSize={this.state.snapshotsTrueSize[snapshotName]} 
                  listSnapshots={this._listSnapshots}
                  endpoint={this.props.endpointStatus.endpoint}
                  notificationSystem={this.props.notificationSystem}
                  clusterName={this.props.clusterName}
                  key={snapshotName}/>
      );
  
      const takeSnapshotClick = (
        <Popover id="takeSnapshot" title="Confirm?">
          <strong>You want to take a snapshot:</strong><br/>
          <button type="button" className="btn btn-xs btn-success" onClick={this._takeSnapshot}>On this node</button>&nbsp;
          <button type="button" className="btn btn-xs btn-success" onClick={this._takeSnapshotClusterWide}>On all nodes in the cluster</button>
        </Popover>
      );
  
      return (
            <span style={displayNodeStyle}>
              <OverlayTrigger placement="top" overlay={tooltip}><button type="button" style={btStyle} className={buttonStyle} onClick={this.open}>&nbsp;</button></OverlayTrigger>
              <Modal show={this.state.showModal} onHide={this.close} bsSize="large" aria-labelledby="contained-modal-title-lg" dialogClassName="large-modal">
                <Modal.Header closeButton>
                  <Modal.Title>Endpoint {this.props.endpointStatus.endpoint}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                  <div className="row">
                    <div className="col-lg-3">
                      <h4>Host id</h4>
                      <p>{this.props.endpointStatus.hostId}</p>
                    </div>
                    <div className="col-lg-3">
                      <h4>Datacenter / Rack</h4>
                      <p>{this.props.endpointStatus.dc} / {this.props.endpointStatus.rack}</p>
                    </div>
                    <div className="col-lg-3">
                      <h4>Release version</h4>
                      <p>{this.props.endpointStatus.releaseVersion}</p>
                    </div>
                    <div className="col-lg-3">
                      <h4>Tokens</h4>
                      <p><OverlayTrigger trigger="click" placement="bottom" overlay={tokens}><button type="button" className="btn btn-md btn-success" style={takeSnapshotStyle}>{this.state.tokens.length}</button></OverlayTrigger></p>
                    </div>
                    <div className="col-lg-3">
                      <h4>Status</h4>
                      <p><button type="button" className={largeButtonStyle}>{this.props.endpointStatus.status}</button></p>
                    </div>
                    <div className="col-lg-3">                  
                      <h4>Severity</h4>
                      <p>{this.props.endpointStatus.severity}</p>
                    </div>
                    <div className="col-lg-3">                  
                      <h4>Data size on disk</h4>
                      <p>{humanFileSize(this.props.endpointStatus.load, 1024)}</p>
                    </div>
                  </div>
                  <div className="row">
                  <div className="col-lg-12">
                  <Tabs defaultActiveKey={1} id="node-tab">
                    <Tab eventKey={1} title="Thread pool stats">
                      <TpStats endpoint={this.props.endpointStatus.endpoint}
                        clusterName={this.props.clusterName}/>
                    </Tab>
                    <Tab eventKey={2} title="Dropped messages">
                      <DroppedMessages endpoint={this.props.endpointStatus.endpoint}
                        clusterName={this.props.clusterName}/>
                    </Tab>
                    <Tab eventKey={3} title="Client Latency">
                      <ClientRequestLatency endpoint={this.props.endpointStatus.endpoint}
                        clusterName={this.props.clusterName}/>
                    </Tab>
                    <Tab eventKey={4} title="Compactions">
                      <ActiveCompactions endpoint={this.props.endpointStatus.endpoint}
                        clusterName={this.props.clusterName}/>
                    </Tab>
                    <Tab eventKey={5} title="Snapshots">
                      <div className="panel panel-success">
                        <div className="panel-heading">
                          <div className="panel-title">
                            <div className="row">
                              <div className="col-lg-8"><h4>Snapshots </h4></div><div className="col-lg-4"><h4><span className="label label-primary">Size on disk: {humanFileSize(this.state.totalSnapshotSizeOnDisk, 1024)}</span> <span className="label label-warning">True size: {humanFileSize(this.state.totalSnapshotTrueSize, 1024)}</span></h4></div>
                            </div>
                          </div>
                        </div>
                        <div className="panel-body" id="snapshots">
                        <div className="row">
                          <div className="col-lg-12">
                            <OverlayTrigger trigger="focus" placement="bottom" overlay={takeSnapshotClick}><button type="button" className="btn btn-md btn-success" style={takeSnapshotStyle}>Take a snapshot</button></OverlayTrigger>                  
                            <button type="button" className="btn btn-md btn-success" style={progressStyle} disabled>Taking a snapshot...</button>
                          </div>
                          <div className="col-lg-12">&nbsp;</div> 
                            {snapshots}
                          </div>
                        </div>
                      </div>
                      </Tab>
                      <Tab eventKey={6} title="Streams">
                          <Streams endpoint={this.props.endpointStatus.endpoint} clusterName={this.props.clusterName}/>
                      </Tab>
                    </Tabs>               
                    </div>
                    </div>
                  
                </Modal.Body>
                <Modal.Footer>
                  <Button onClick={this.close}>Close</Button>
                </Modal.Footer>
              </Modal>            
            </span>
            
      );
  
      }
  
  })
  
export default NodeStatus;
