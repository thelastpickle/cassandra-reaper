import React from "react";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import NavBar from "jsx/navbar";
import NodeMultiSelect from "jsx/node-multiselect";
import DiagEventsList from "jsx/event-list";
import StatusUpdateMixin from "jsx/mixin";
import URL_PREFIX from "jsx/uicommon";

const eventScreen = React.createClass({
  mixins: [StatusUpdateMixin],

  propTypes: {
    currentCluster: React.PropTypes.string.isRequired,
    clusterNames: React.PropTypes.object.isRequired,
    diagnosticEvents: React.PropTypes.object.isRequired,
    getClusterStatusSubject: React.PropTypes.object.isRequired,
    clusterStatusResult: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {
      clusterName: this.props.currentCluster == "undefined" ? "all" : this.props.currentCluster,
      clusterNames: [],
      endpointsByClusterAndRack: {}
    };
  },

  componentWillMount: function() {
    // trigger cluster status results (clusterStatusResult) for multi-select child component
    // this needs to be done for each registered cluster
    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => {
        let previousNames = this.state.clusterNames;
        this.setState({clusterNames: names});
        if(names.length == 1) this.setState({clusterName: names[0]});
        if(names.length > 0 && previousNames.length == 0) {
          names.map(name => this.props.getClusterStatusSubject.onNext(name));
        }
      })
    );
  },

  componentWillUnmount: function() {
    this._clusterNamesSubscription.dispose();
  },

  componentDidMount: function() {
    $('#events-selection').multiselect({
//      enableFiltering: true
        enableClickableOptGroups: true,
        onChange: this._onNodeSelection
    });

    this._subscribe();
  },

  _subscribe: function() {
    const comp = this;

    const isDev = window != window.top;
    const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';

    $.ajax({
      method: "POST",
      url: `${URL_PREFIX}/events/subscribe`,
      data: { clusterName: comp.state.currentCluster }
    }).done(function(msg) {
      console.debug(msg);
    });
  },

  _onEventSelection: function(option, checked, select) {
    // TODO
  },

  _onNodeSelection: function(option, checked, select) {
    // TODO
  },


  render: function() {

    return (
      <div id="wrapper">
          <!-- Navigation -->
          <nav className="navbar navbar-default navbar-static-top" role="navigation">
            <NavBar></NavBar>
            <!-- /.navbar-header -->

            <Sidebar clusterNames={this.props.clusterNames} currentCluster={this.state.clusterName}></Sidebar>
          </nav>

          <div id="page-wrapper">
            <div className="row">
              <ServerStatus statusObservableTimer={this.props.statusObservableTimer}></ServerStatus>
            </div>
            <div className="row">
              <div className="col-lg-12">
                  <h1 className="page-header">Diagnostic Events</h1>
              </div>
              <!-- /.col-lg-12 -->
            </div>
            <!-- /.row -->

            <div className="row">
              <div className="col-lg-2">
                <NodeMultiSelect clusterStatusResult={this.props.clusterStatusResult} onChange={this._onEventSelection}/>
              </div>
              <div className="col-lg-2">
                <label style={{paddingRight: '1em'}} htmlFor="events-selection">Events:</label>
                <select id="events-selection" multiple="multiple">
                  <!-- optgroup label="Group 1" -->
                  <option value="org.apache.cassandra.transport.messages.AuthEvent">AuthEvent</option>
                  <option value="org.apache.cassandra.dht.BootstrapEvent">BootstrapEvent</option>
                  <option value="org.apache.cassandra.transport.messages.CQLAuditEvent">CQLAuditEvent</option>
                  <option value="org.apache.cassandra.gms.GossiperEvent">GossiperEvent</option>
                  <option value="org.apache.cassandra.hints.HintEvent">HintEvent</option>
                  <option value="org.apache.cassandra.hints.HintsServiceEvent">HintsServiceEvent</option>
                  <option value="org.apache.cassandra.service.PendingRangeCalculatorServiceEvent">PendingRangeCalculatorServiceEvent</option>
                  <option value="org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent">TokenAllocatorEvent</option>
                  <option value="org.apache.cassandra.locator.TokenMetadataEvent">TokenMetadataEvent</option>
                </select>
              </div>
            </div>
            <div className="row">
              <div className="col-lg-12">
                <DiagEventsList diagnosticEvents={this.props.diagnosticEvents}></DiagEventsList>
              </div>
            </div>
          </div>
          <!-- /#page-wrapper -->
      </div>
    );
  }

});

export default eventScreen;