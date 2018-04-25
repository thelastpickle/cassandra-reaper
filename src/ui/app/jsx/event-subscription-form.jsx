import $ from "jquery";
import React from "react";
import NodeMultiSelect from "jsx/node-multiselect";


const subscriptionForm = React.createClass({

  propTypes: {
    addSubscriptionSubject: React.PropTypes.object.isRequired,
    listenSubscriptionSubject: React.PropTypes.object.isRequired,
    addSubscriptionResult: React.PropTypes.object.isRequired,
    clusterStatusResult: React.PropTypes.object.isRequired,
  },

  getInitialState: function() {
    const isDev = window.top.location.pathname.includes('webpack-dev-server');
    const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';

    return {
      addSubscriptionResultMsg: null, submitEnabled: false, clusterName: null,
      description: null, export_sse: false, export_logger: null, export_http: null,
      nodes_selection: [], events_selection: [],
      formCollapsed: true,
      datacenters: null, datacenters: "", datacenterList: [], datacenterSuggestions: []
    };
  },

  componentWillMount: function() {
    this._scheduleResultSubscription = this.props.addSubscriptionResult.subscribeOnNext(obs =>
      obs.subscribe(
        r => this.setState({addSubscriptionResultMsg: null}),
        r => this.setState({addSubscriptionResultMsg: r.responseText})
      )
    );

    this.props.clusterStatusResult.subscribeOnNext(obs => {
      obs.subscribeOnNext(status => {
        this.setState({clusterName: status.name});
      });
    });
  },

  componentWillUnmount: function() {
    this._scheduleResultSubscription.dispose();
  },

  _onAdd: function(e) {
    const sub = {
      clusterName: this.state.clusterName,
      description: this.state.description,
      includeNodes: this.state.nodes_selection.join(","),
      events: this.state.events_selection.join(","),
      exportSse: this.state.export_sse
    };
    if(this.state.export_logger) sub.exportFileLogger = this.state.export_logger;
    if(this.state.export_http) sub.exportHttpEndpoint = this.state.export_http;

    this.props.addSubscriptionSubject.onNext(sub);
  },

//  _onAddAndView: function(e) {
//    var resultSubscription = this.props.addSubscriptionResult.subscribeOnNext(s => {
//         resultSubscription.dispose();
//         s.flatMap(r => {
//           listenSubscriptionSubject.onNext(r.id);
//         });
//       }
//    );
//    this._onAdd(e);
//  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix
    if(n == "export_sse") {
      v = e.target.checked === true;
    }

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

  _onEventsSelection: function(element, checked) {
    const opts = element.target.selectedOptions;
    const ov = [];
    for(var i = 0; i < opts.length; i++) {
      ov.push(opts[i].value);
    }
    this.setState({events_selection: ov});
    this._checkValidity();
  },

  _onNodeSelection: function(element, checked) {
    var c = element.val();
    if(checked) {
      const nodes = this.state.nodes_selection.concat(c);
      this.setState({nodes_selection: nodes});
    } else {
      const nodes = this.state.nodes_selection.filter(node => node != c);
      this.setState({nodes_selection: nodes});
    }
    this._checkValidity();
  },

  _checkValidity: function() {
    const valid = this.state.description
      && this.state.events_selection && this.state.events_selection.length > 0
      && this.state.nodes_selection && this.state.nodes_selection.length > 0
      && (this.state.export_sse === true || this.state.export_logger
      || (this.state.export_http && (this.state.export_http.startsWith("http://")
                                     || this.state.export_http.startsWith("https://"))));

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

    let addMsg = null;
    if(this.state.addSubscriptionResultMsg) {
      addMsg = <div className="alert alert-danger" role="alert">{this.state.addSubscriptionResultMsg}</div>
    }

    const form = <div className="row">
        <div className="col-lg-12">

          <form className="form-horizontal form-condensed">
            <div className="col-sm-offset-1 col-sm-11">
            <div className="form-group">
                <label htmlFor="in_description" className="col-sm-3 control-label">Description</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="text" className="form-control" value={this.state.description}
                    onChange={this._handleChange} id="in_description" placeholder="Name of subscription"/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_nodes" className="col-sm-3 control-label">Nodes</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <NodeMultiSelect clusterStatusResult={this.props.clusterStatusResult} onChange={this._onNodeSelection}/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="events-selection" className="col-sm-3 control-label">Events:</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <select id="in_events_selection" multiple="multiple" onChange={this._onEventsSelection} className="form-control">
                    <!-- optgroup label="Group 1" -->
                    <option value="org.apache.cassandra.audit.AuditEvent">AuditEvent</option>
                    <option value="org.apache.cassandra.dht.BootstrapEvent">BootstrapEvent</option>
                    <option value="org.apache.cassandra.gms.GossiperEvent">GossiperEvent</option>
                    <option value="org.apache.cassandra.hints.HintEvent">HintEvent</option>
                    <option value="org.apache.cassandra.hints.HintsServiceEvent">HintsServiceEvent</option>
                    <option value="org.apache.cassandra.service.PendingRangeCalculatorServiceEvent">PendingRangeCalculatorServiceEvent</option>
                    <option value="org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent">TokenAllocatorEvent</option>
                    <option value="org.apache.cassandra.locator.TokenMetadataEvent">TokenMetadataEvent</option>
                  </select>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_export_sse" className="col-sm-3 control-label">Enable Live View</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="checkbox" id="in_export_sse" onChange={this._handleChange} value={this.state.export_sse}/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_export_logger" className="col-sm-3 control-label">Export file logger</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="text" className="form-control" value={this.state.export_logger}
                    onChange={this._handleChange} id="in_export_logger" placeholder="Name of logger used for file export"/>
                </div>
              </div>
              <div className="form-group">
                <label htmlFor="in_export_http" className="col-sm-3 control-label">Export HTTP endpoint</label>
                <div className="col-sm-14 col-md-12 col-lg-9">
                  <input type="url" className="form-control" value={this.state.export_http}
                    onChange={this._handleChange} id="in_export_http" placeholder="URL endpoint used for posting events"/>
                </div>
              </div>
            </div>
            <div className="col-sm-offset-3 col-sm-9">
              <button type="button" className="btn btn-success" disabled={!this.state.submitEnabled}
                onClick={this._onAdd}>Save</button>
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

    const formHeader = <div className="panel-title" >
                          <a href="#schedule-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>Add Events Subscription</a>
                          &nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span>
                                 <span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span></div>



    return (<div className="panel panel-warning">
              <div className="panel-heading">
                {formHeader}
              </div>
              <div className="panel-body collapse" id="schedule-form">
                {addMsg}
                {form}
              </div>
            </div>);
  }
});

export default subscriptionForm;
