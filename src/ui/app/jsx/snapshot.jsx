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
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import Modal from 'react-bootstrap/lib/Modal';
import Button from 'react-bootstrap/lib/Button';
import Tooltip from 'react-bootstrap/lib/Tooltip';
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger';
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import Popover from 'react-bootstrap/lib/Popover';
import $ from "jquery";
var NotificationSystem = require('react-notification-system');

const Snapshot = React.createClass({
    propTypes: {
      snapshotName: React.PropTypes.string.isRequired,
      snapshots: React.PropTypes.array.isRequired,
      totalSizeOnDisk: React.PropTypes.number,
      totalTrueSize: React.PropTypes.number,
      listSnapshots: React.PropTypes.func.isRequired,
      endpoint: React.PropTypes.string.isRequired,
      notificationSystem: React.PropTypes.object,
      clusterName: React.PropTypes.string.isRequired
    },

    getInitialState() {
      return {communicating: false, collapsed: true};
    },
  
    clearOnThisNode: function() {
      this.setState({communicating: true});
      toast(this.props.notificationSystem, "Clearing snapshot " + this.props.snapshotName + " on node " + this.props.endpoint, "warning", this.props.snapshotName);
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname) + '/snapshot/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpoint) + "/" + this.props.snapshotName,
        method: 'DELETE',
        component: this,
        dataType: 'text',
        success: function(data) {
            toast(this.component.props.notificationSystem, "Successfully cleared snapshot " + this.component.props.snapshotName + " on node " + this.component.props.endpoint, "success", this.component.props.snapshotName);
        },
        complete: function(data) {
          this.component.props.listSnapshots();
          this.component.setState({communicating: false});
        },
        error: function(data) {
            toast(this.component, props.notificationSystem, "Failed clearing snapshot " + this.component.props.snapshotName + " on node " + this.component.props.endpoint + "<br/>" + data.responseText, "error", this.component.props.snapshotName);
        }
    })
    },
  
    clearOnAllNodes: function() {
      this.setState({communicating: true});
      toast(this.props.notificationSystem, "Clearing snapshot " + this.props.snapshotName + " on cluster " + this.props.clusterName, "warning", this.props.snapshotName);
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname) + '/snapshot/cluster/' + encodeURIComponent(this.props.clusterName) + "/" + this.props.snapshotName,
        method: 'DELETE',
        component: this,
        dataType: 'text',
        success: function(data) {
            toast(this.component.props.notificationSystem, "Successfully cleared snapshot " + this.component.props.snapshotName + " on cluster " + this.component.props.clusterName, "success", this.component.props.snapshotName);
        },
        complete: function(data) {
          this.component.props.listSnapshots();
          this.component.setState({communicating: false});
        },
        error: function(data) {
            toast(this.component.props.notificationSystem, "Failed clearing snapshot " + this.component.props.snapshotName + " on cluster " + this.component.props.clusterName + "<br/>" + data.responseText, "error", this.component.props.snapshotName);
        }
    })
    },
  
    _toggleDisplay: function() {
      if(this.state.collapsed == true) {
        this.setState({collapsed: false});
      }
      else {
        this.setState({collapsed: true});
      }
    },
  
    render: function() {
  
      // sort function
      function compareSnapshots(a,b) {
        if (a.keyspace+"."+a.table < b.keyspace+"."+b.table)
          return -1;
        if (a.keyspace+"."+a.table > b.keyspace+"."+b.table)
          return 1;
        return 0;
      };
  
      let progressStyle = {
        display: "none" 
      }
  
      let yesStyle = {
        display: "inline-block" 
      }
  
      if (this.state.communicating==true) {
        progressStyle = {
          display: "inline-block"
        }
        yesStyle = {
          display: "none"
        } 
      }
  
      // Handle styles for collapsing the snapshot panel
      let snapshotPanelDownStyle = {
        display: "none" 
      }
  
      let snapshotPanelUpStyle = {
        display: "inline-block" 
      }
  
      if(this.state.collapsed == true) {
        snapshotPanelDownStyle = {
          display: "inline-block"
        }
        snapshotPanelUpStyle = {
          display: "none"
        }
      }
      
  
      const clearThisNodeClick = (
        <Popover id={this.props.snapshotName + "-clearnode"} title="Confirm?">
          <strong>You want to clear this snapshot:</strong>&nbsp;
          <button type="button" className="btn btn-xs btn-danger" onClick={this.clearOnThisNode} style={yesStyle}>On this node</button>
          <button type="button" className="btn btn-xs btn-danger" style={progressStyle} disabled>Deleting...</button>&nbsp;
          <button type="button" className="btn btn-xs btn-danger" onClick={this.clearOnAllNodes} style={yesStyle}>On all nodes</button>
          <button type="button" className="btn btn-xs btn-danger" style={progressStyle} disabled>Deleting...</button>
        </Popover>
      );
  
      const tables = this.props.snapshots.sort(compareSnapshots).map(snapshot => 
      <div className="row">
        <div className="col-lg-8">{snapshot.keyspace}.{snapshot.table}</div>
        <div className="col-lg-4"><span className="label label-primary">{humanFileSize(snapshot.sizeOnDisk, 1024)}</span> <span className="label label-warning">{humanFileSize(snapshot.trueSize, 1024)}</span></div>
      </div>
      );
  
      return (<div className="col-lg-12"> 
                <div className="panel panel-default">
                <div className="panel-heading">
                  <div className="row">
                    <div className="col-lg-8"><button href={"#snapshot-" + this.props.snapshotName} data-toggle="collapse" onClick={this._toggleDisplay} className="btn btn-md btn-default">{this.props.snapshotName}&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={snapshotPanelDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={snapshotPanelUpStyle}></span></button>
                    </div>
                    <div className="col-lg-3"><h5><span className="label label-primary">Size on disk: {humanFileSize(this.props.totalSizeOnDisk, 1024)}</span>&nbsp;<span className="label label-warning">True size: {humanFileSize(this.props.totalTrueSize, 1024)}</span></h5></div>
                    <div className="col-lg-1"><OverlayTrigger trigger="focus" placement="bottom" overlay={clearThisNodeClick}><h5><button type="button" className="btn btn-xs btn-danger">Delete</button></h5></OverlayTrigger></div>
                  </div>
                </div>
                  <div className="panel-body collapse" id={"snapshot-" + this.props.snapshotName}>
                    {tables}
                  </div>
                </div>
              </div>
      );
    }
  })

  export default Snapshot;
