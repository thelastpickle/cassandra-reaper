import React from "react";
import $ from "jquery";
import {DeleteStatusMessageMixin} from "jsx/mixin";
import Modal from 'react-bootstrap/lib/Modal';
import Button from 'react-bootstrap/lib/Button';
import Tooltip from 'react-bootstrap/lib/Tooltip';
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger';
import ProgressBar from 'react-bootstrap/lib/ProgressBar';

const NodeStatus = React.createClass({
  propTypes: {
    endpointStatus: React.PropTypes.object.isRequired,
    clusterName: React.PropTypes.string.isRequired,
    nbNodes: React.PropTypes.number.isRequired,
    rackLoad: React.PropTypes.number.isRequired
  },

  getInitialState() {
    return { showModal: false };
  },

  close() {
    this.setState({ showModal: false });
  },

  open() {
    this.setState({ showModal: true });
  },

  render: function() {
    
    let buttonStyle = "btn btn-xs btn-success";
    let largeButtonStyle = "btn btn-lg btn-success";

    if(!this.props.endpointStatus.status.endsWith('UP')){
      buttonStyle = "btn btn-xs btn-danger";
      largeButtonStyle = "btn btn-lg btn-danger";
    }

    const btStyle = {
      width: (((this.props.endpointStatus.load/this.props.rackLoad)*100)-0) + "%",
      margin:"0px",
      textOverflow: "hidden"
    };

    const tooltip = (
      <Tooltip id="tooltip"><strong>{this.props.endpointStatus.endpoint}</strong> ({humanFileSize(this.props.endpointStatus.load, 1024)})</Tooltip>
    );

    return (<span>
            <OverlayTrigger placement="top" overlay={tooltip}><button type="button" style={btStyle} className={buttonStyle} onClick={this.open}>{this.props.endpointStatus.endpoint} ({humanFileSize(this.props.endpointStatus.load, 1024)})</button></OverlayTrigger>
            <Modal show={this.state.showModal} onHide={this.close}>
              <Modal.Header closeButton>
                <Modal.Title>Endpoint {this.props.endpointStatus.endpoint}</Modal.Title>
              </Modal.Header>
              <Modal.Body>
                <h4>Host id</h4>
                <p>{this.props.endpointStatus.hostId}</p>
                <h4>Datacenter / Rack</h4>
                <p>{this.props.endpointStatus.dc} / {this.props.endpointStatus.rack}</p>
                <h4>Release version</h4>
                <p>{this.props.endpointStatus.releaseVersion}</p>
                <h4>Tokens</h4>
                <p>{this.props.endpointStatus.tokens}</p>
                <h4>Status</h4>
                <p><button type="button" className={largeButtonStyle}>{this.props.endpointStatus.status}</button></p>
                <h4>Severity</h4>
                <p>{this.props.endpointStatus.severity}</p>
                <h4>Data size on disk</h4>
                <p>{humanFileSize(this.props.endpointStatus.load, 1024)}</p>
              </Modal.Body>
              <Modal.Footer>
                <Button onClick={this.close}>Close</Button>
              </Modal.Footer>
            </Modal>
          </span>
    );

    }

})


const Cluster = React.createClass({

  propTypes: {
    name: React.PropTypes.string.isRequired
  },
  
  getInitialState: function() {
    const isDev = window.top.location.pathname.includes('webpack-dev-server');
    const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';
    return {clusterStatus: {}, clusterStatuses: null, urlPrefix: URL_PREFIX , nbNodes: 0, nodesDown:0};
  },

  componentWillMount: function() {
    this._refreshClusterStatus();
    this.setState({clusterStatuses: setInterval(this._refreshClusterStatus, 10000)}); 
    console.log("Path " + window.location.pathname);
  },

  _refreshClusterStatus: function() {
    $.ajax({
          url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(this.props.name),
          method: 'GET',
          component: this,
          complete: function(data) {
            this.component.setState({clusterStatus: $.parseJSON(data.responseText)});
          }
      });
  },

  componentWillUnmount: function() {
    clearInterval(this.clusterStatuses);
  },

  render: function() {

    let rowDivStyle = {
      marginLeft: "0",
      marginRight: "0"
    };

    let progressStyle = {
      marginTop: "0.25em",
      marginBottom: "0.25em"
    }

    let datacenters=<div className="clusterLoader"></div>

    let runningRepairs = 0;

    let repairProgress = "";
    let totalLoad = 0;
    if(this.state.clusterStatus.nodes_status){
      runningRepairs = this.state.clusterStatus.repair_runs.reduce(function(previousValue, repairRun){
                              return previousValue + (repairRun.state=='RUNNING' ? 1: 0); 
                            }, 0);;

      repairProgress = this.state.clusterStatus.repair_runs.filter(repairRun => repairRun.state=='RUNNING').map(repairRun => 
                      <ProgressBar now={(repairRun.segments_repaired*100)/repairRun.total_segments} active bsStyle="success" 
                                   style={progressStyle} 
                                   label={repairRun.keyspace_name}
                                   key={repairRun.id}/>
      )

      datacenters = Object.keys(this.state.clusterStatus.nodes_status.endpointStates[0].endpoints).sort().map(dc => 
                      <Datacenter datacenter={this.state.clusterStatus.nodes_status.endpointStates[0].endpoints[dc]} 
                                  datacenterName={dc} 
                                  nbDatacenters={Object.keys(this.state.clusterStatus.nodes_status.endpointStates[0].endpoints).length} 
                                  clusterName={this.props.name} key={this.props.name + '-' + dc} 
                                  totalLoad={this.state.clusterStatus.nodes_status.endpointStates[0].totalLoad}/>
      )

      totalLoad = this.state.clusterStatus.nodes_status.endpointStates[0].totalLoad;
    }

    let runningRepairsBadge = <span className="label label-default">{runningRepairs}</span>;
    if(runningRepairs > 0) {
      runningRepairsBadge = <span className="label label-success">{runningRepairs}</span>;
    }

    return (
      <div className="panel panel-default">
        <div className="panel-body">
          <div className="row">
              <div className="col-lg-2"><a href={'repair.html?currentCluster=' + this.props.name}><h4>{this.props.name} <span className="badge">{humanFileSize(totalLoad,1024)}</span></h4></a><div>Running repairs : {runningRepairsBadge}<br/>{repairProgress}</div>
              <button type="button" className="btn btn-xs btn-danger" onClick={this._onDelete}>Delete cluster</button>
              </div>
              <div className="col-lg-10">
              <div className="row" style={rowDivStyle}>
                <div className="row" style={rowDivStyle}>
                      {datacenters}
                </div>
              </div>
              </div>
          </div>
          </div>
        </div>
    );
  },

  _onDelete: function(e) {
    this.props.deleteSubject.onNext(this.props.name);
  }

});


const Datacenter = React.createClass({

  propTypes: {
    datacenter: React.PropTypes.object.isRequired,
    datacenterName: React.PropTypes.string.isRequired,
    nbDatacenters: React.PropTypes.number.isRequired,
    clusterName: React.PropTypes.string.isRequired,
    totalLoad: React.PropTypes.number.isRequired
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
      width: (((dcSize/this.props.totalLoad)*100)) + "%"
    };

    let badgeStyle = {
      float: "right"
    }

    let panelHeadingStyle = {
      padding: "2px 10px"
    };

    let panelBodyStyle = {
      padding: "1px"
    };

    let panelStyle = {
      marginBottom: "1px"
    }

    const nbRacks = Object.keys(this.props.datacenter).length;
    const racks = Object.keys(this.props.datacenter).sort().map(rack => 
          <Rack key={this.props.datacenterName+'-'+rack} rack={this.props.datacenter[rack]} nbRacks={nbRacks} clusterName={this.props.clusterName} dcLoad={dcSize}/>);
    return (
            <div className="col-lg-12" style={rowDivStyle}>
              <div className="panel panel-default panel-info" style={panelStyle}>
                <div className="panel-heading" style={panelHeadingStyle}><b>{this.props.datacenterName} <span className="badge" style={badgeStyle}>{humanFileSize(dcSize, 1024)}</span></b></div>
                <div className="panel-body" style={panelBodyStyle}>{racks}</div>
              </div>
            </div>
    );
  },


});

const Rack = React.createClass({

  propTypes: {
    rack: React.PropTypes.array.isRequired,
    nbRacks: React.PropTypes.number.isRequired,
    clusterName: React.PropTypes.string.isRequired,
    dcLoad: React.PropTypes.number.isRequired
  },
  
  render: function() {

  const rackSize = this.props.rack.reduce(function(previousValue, endpoint){
                              return previousValue + endpoint.load; 
                            }, 0);;
  let rowDivStyle = {
      marginLeft: "0",
      paddingLeft: "0",
      paddingRight: "1px",
      width: ((rackSize/this.props.dcLoad)*100) + "%"
  };

  let badgeStyle = {
    float: "right"
  }

  let panelHeadingStyle = {
    padding: "2px 10px"
  };

  let panelBodyStyle = {
    padding: "1px"
  };

  let panelStyle = {
    marginBottom: "1px"
  }
  
  let nodes= "" ;
  let rackName = "";
  

  if(this.props.rack) {
    rackName = this.props.rack[0].rack;
    nodes = this.props.rack.map(endpoint =>
        <NodeStatus key={endpoint.endpoint} endpointStatus={endpoint} clusterName={this.props.clusterName} nbNodes={this.props.rack.length} rackLoad={rackSize}/>
    );
  }

  return (
            <div className="col-lg-12" style={rowDivStyle}>
              <div className="panel panel-default panel-success" style={panelStyle}>
                <div className="panel-heading" style={panelHeadingStyle}><b>{rackName} <span className="badge" style={badgeStyle}>{humanFileSize(rackSize, 1024)}</span></b></div>
                <div className="panel-body" style={panelBodyStyle}>{nodes}</div>
              </div>
            </div>
  );
  },


});



const clusterList = React.createClass({
  mixins: [DeleteStatusMessageMixin],

  propTypes: {
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {clusterNames: [], deleteResultMsg: null, clusterFilter: ""};
  },

  componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => this.setState({clusterNames: names}))
    );
  },

  componentWillUnmount: function() {
    this._clusterNamesSubscription.dispose();
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix
    
    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);
  }, 

  render: function() {

    const rows = this.state.clusterNames.filter(cluster => cluster.includes(this.state.clusterFilter))
                                        .sort().map(name =>
      <Cluster name={name} key={name} deleteSubject={this.props.deleteSubject} getClusterStatus={this.props.getClusterStatus} getClusterSubject={this.props.getClusterSubject}/>);

    let table = null;
    if(rows.length == 0) {
      table = <div className="alert alert-info" role="alert">No clusters found</div>
    } else {

      table = <div>
              {rows}
              </div>
    }

    let filterInput = <div className="row">
    <div className="col-lg-12">
      <form className="form-horizontal form-condensed">
        <div className="form-group">
          <label htmlFor="in_clusterName" className="col-sm-3 control-label">Filter: </label>
          <div className="col-sm-9 col-md-7 col-lg-5">
            <input type="text" required className="form-control" value={this.state.clusterFilter}
                  onChange={this._handleChange} id="in_clusterFilter" placeholder="Start typing to filter clusters..."/>
          </div>
        </div>
      </form>
    </div>
    </div>

    return (<div className="row">
              <div className="col-lg-12">
                {this.deleteMessage()}
                {filterInput}
                {rows}
              </div>
            </div>);
  }
});

const humanFileSize = function(bytes, si) {
      var thresh = si ? 1000 : 1024;
      if(Math.abs(bytes) < thresh) {
          return bytes + ' B';
      }
      var units = si
          ? ['kB','MB','GB','TB','PB','EB','ZB','YB']
          : ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
      var u = -1;
      do {
          bytes /= thresh;
          ++u;
      } while(Math.abs(bytes) >= thresh && u < units.length - 1);
      return bytes.toFixed(1)+' '+units[u];
    }

export default clusterList;