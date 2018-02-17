import React from "react";
import { WithContext as ReactTags } from 'react-tag-input';
import {getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";
var NotificationSystem = require('react-notification-system');

const snapshotForm = React.createClass({
  _notificationSystem: null,

  propTypes: {
    clusterNames: React.PropTypes.object.isRequired,
    listSnapshots: React.PropTypes.func,
    changeCurrentCluster: React.PropTypes.func
  },

  getInitialState: function() {
    const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
    
    return {
      clusterNames: [], submitEnabled: false,
      clusterName: this.props.currentCluster!="all"?this.props.currentCluster:this.props.clusterNames[0], 
      keyspace: "", tables: "", owner: "", name: "",
      cause: "", formCollapsed: true, 
      clusterStatus: {}, urlPrefix: URL_PREFIX, tableSuggestions: [], 
      clusterTables: {}, tableList: [], keyspaceList: [], keyspaceSuggestions: [],
      tablelistReadOnly: false
    };
  },

  componentWillMount: function() {
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => {
        let previousNames = this.state.clusterNames;
        this.setState({clusterNames: names});
        if(names.length == 1) this.setState({clusterName: names[0]});
        if(previousNames.length == 0) {
          this._getClusterStatus();
        }
      })
    );
  },

  componentWillUnmount: function() {
    this._clusterNamesSubscription.dispose();
  },

  componentDidMount: function() {
    this._notificationSystem = this.refs.notificationSystem;
  },

  _getClusterStatus: function() {
    let clusterName = this.state.clusterName;
    $.ajax({
          url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(clusterName),
          method: 'GET',
          component: this,
          complete: function(data) {
            this.component.setState({clusterStatus: $.parseJSON(data.responseText)});
          }
      });
    $.ajax({
      url: this.state.urlPrefix + '/cluster/' + encodeURIComponent(clusterName) + '/tables',
      method: 'GET',
      component: this,
      complete: function(data) {
        this.component.setState({clusterTables: $.parseJSON(data.responseText)});
        this.component._getKeyspaceSuggestions();
      }
    });
  },

  _takeSnapshot: function(snapshot) {
      this.setState({communicating: true}); 
      toast(this._notificationSystem, "Taking a new snapshot...", "warning", snapshot.clusterName);  
      const snapshotParams = '&snapshot_name=' + encodeURIComponent(snapshot.name==''?'reaper':snapshot.name) 
                           + '&owner=' + encodeURIComponent(snapshot.owner)
                           + '&cause=' + encodeURIComponent(snapshot.cause);
      
      $.ajax({
            url: getUrlPrefix(window.top.location.pathname) + '/snapshot/cluster/' + encodeURIComponent(snapshot.clusterName) + "?keyspace=" + snapshot.keyspace + snapshotParams,
            dataType: 'text',
            method: 'POST',
            component: this,
            success: function(data) {
                toast(this.component._notificationSystem, "Successfully took a new snapshot", "success", snapshot.clusterName); 
            },
            complete: function(data) {
              this.component.setState({communicating: false});
              this.component.props.changeCurrentCluster(snapshot.clusterName);
              this.component.props.listSnapshots(snapshot.clusterName);
            },
            error: function(data) {
                toast(this.component._notificationSystem, "Failed taking a snapshot on cluster " + snapshot.clusterName + " : " + data.responseText, "error", snapshot.clusterName);
            }
        });
  },

  _getKeyspaceSuggestions: function() {
    this.setState({keyspaceSuggestions: Object.keys(this.state.clusterTables)});
  },

  _getTableSuggestions: function(ks) {
    this.setState({tableSuggestions: this.state.clusterTables[ks]});
  },

  _onAdd: function(e) {
    const snapshot = {
      clusterName: this.state.clusterName, 
      owner: this.state.owner,
      name: this.state.name,
      keyspace: ""
    };
    if(this.state.keyspace) snapshot.keyspace = this.state.keyspace;
    if(this.state.cause) snapshot.cause = this.state.cause;

    this._takeSnapshot(snapshot);
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);
    
    if (n == 'clusterName') {
      this._getClusterStatus();
    }
    
    // validate
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.clusterName && this.state.owner;
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

    // Keyspace tag list functions
    _handleKeyspaceAddition(ks) {
      let keyspaces = this.state.keyspaceList;
      if (keyspaces.length==0) {
        if ($.inArray(ks, this.state.keyspace.split(','))==-1) {
          keyspaces.push({
              id: this._create_UUID(),
              text: ks
          });
          this.setState({keyspaceList: keyspaces, keyspace: ks, keyspaces: keyspaces.map(ks => ks.text).join(',')});
          this._checkValidity();
          this._getTableSuggestions(ks);
        }
      }
    },
  
    _handleKeyspaceDelete(i) {
        let keyspaces = this.state.keyspaceList;
        keyspaces.splice(i, 1);
        this.setState({keyspaceList: keyspaces, keyspace: "", keyspaces: keyspaces.map(ks => ks.text).join(',')});
        this._checkValidity();
        this._getTableSuggestions("");
    },

    _handleKeyspaceFilterSuggestions(textInputValue, possibleSuggestionsArray) {
      var lowerCaseQuery = textInputValue.toLowerCase();
      let keyspaces = this.state.keyspaceList;
   
      return possibleSuggestionsArray.filter(function(suggestion)  {
          return suggestion.toLowerCase().includes(lowerCaseQuery) && keyspaces.length==0;
      })
    },

    _create_UUID(){
      var dt = new Date().getTime();
      var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
          var r = (dt + Math.random()*16)%16 | 0;
          dt = Math.floor(dt/16);
          return (c=='x' ? r :(r&0x3|0x8)).toString(16);
      });
      return uuid;
    },

  render: function() {

    const clusterItems = this.state.clusterNames.sort().map(name =>
      <option key={name} value={name}>{name}</option>
    );

    const keyspaceInputStyle = this.state.keyspaceList.length > 0 ? 'form-control-hidden':'form-control';

    const form = <div className="row">
        <div className="col-lg-12">

          <form className="form-horizontal form-condensed">

            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Cluster*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_clusterName"
                  onChange={this._handleChange} value={this.state.clusterName}>
                  {clusterItems}
                </select>
              </div>
            </div>

            <div className="form-group">
              <label htmlFor="in_name" className="col-sm-3 control-label">Name</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.name}
                  onChange={this._handleChange} id="in_name" placeholder="name of the snapshot (any string)"/>
              </div>
            </div>

            <div className="form-group">
            <label htmlFor="in_keyspace" className="col-sm-3 control-label">Keyspace</label>
            <div className="col-sm-9 col-md-7 col-lg-5">
              <ReactTags id={'in_keyspace'} tags={this.state.keyspaceList}
                suggestions={this.state.keyspaceSuggestions}
                labelField={'text'} handleAddition={this._handleKeyspaceAddition} 
                handleInputBlur={this._handleKeyspaceAddition} 
                handleDelete={this._handleKeyspaceDelete}
                placeholder={'Empty == all keyspaces'}
                handleFilterSuggestions={this._handleKeyspaceFilterSuggestions}
                classNames={{
                    tagInputField: keyspaceInputStyle
                  }}/>
              </div>
            </div>
            
            <div className="form-group">
              <label htmlFor="in_owner" className="col-sm-3 control-label">Owner*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.owner}
                  onChange={this._handleChange} id="in_owner" placeholder="owner name for the snapshot (any string)"/>
              </div>
            </div>
 

            <div className="form-group">
              <label htmlFor="in_cause" className="col-sm-3 control-label">Cause</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" className="form-control" value={this.state.cause}
                  onChange={this._handleChange} id="in_cause" placeholder="reason snapshot was taken"/>
              </div>
            </div>

            <div className="form-group">
              <div className="col-sm-offset-3 col-sm-9">
                <button type="button" className="btn btn-warning" disabled={!this.state.submitEnabled}
                  onClick={this._onAdd}>Take snapshot</button>
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

    const formHeader = <div className="panel-title" ><a href="#snapshot-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>Take a snapshot</a>&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span></div>





    return (<div className="panel panel-warning">
              <div className="panel-heading">
              <NotificationSystem ref="notificationSystem" />
                {formHeader}
              </div>
              <div className="panel-body collapse" id="snapshot-form">
                {form}
              </div>
            </div>);
  }
});

export default snapshotForm;
