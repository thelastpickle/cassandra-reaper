import React from "react";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import NavBar from "jsx/navbar";
import ClusterSelection from "jsx/cluster-selection";
import DiagEventsSubscriptionForm from "jsx/event-subscription-form";
import DiagEventsSubscriptionList from "jsx/event-subscription-list";
import DiagEventsList from "jsx/event-list";
import URL_PREFIX from "jsx/uicommon";

const eventScreen = React.createClass({

  propTypes: {
    currentCluster: React.PropTypes.string.isRequired,
    clusterNames: React.PropTypes.object.isRequired,
    getClusterStatusSubject: React.PropTypes.object.isRequired,
    // side-bar
    logoutSubject: React.PropTypes.object.isRequired,
    logoutResult: React.PropTypes.object.isRequired,
    // event-subscription-form
    clusterStatusResult: React.PropTypes.object.isRequired,
    addSubscriptionSubject: React.PropTypes.object.isRequired,
    addSubscriptionResult: React.PropTypes.object.isRequired,
    // event-subscription-list
    eventSubscriptions: React.PropTypes.object.isRequired,
    deleteSubscriptionSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    // event-list
    diagnosticEvents: React.PropTypes.object.isRequired,
    listenSubscriptionSubject: React.PropTypes.object.isRequired,
  },

  componentWillMount: function() {
  },

  componentWillUnmount: function() {
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
//    const comp = this;
//
//    const isDev = window != window.top;
//    const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';
//
//    $.ajax({
//      method: "POST",
//      url: `${URL_PREFIX}/events/subscriptions`,
//      data: { clusterName: comp.state.currentCluster }
//    }).done(function(msg) {
//      console.debug(msg);
//    });
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

            <Sidebar
              logoutSubject={this.props.logoutSubject}
              logoutResult={this.props.logoutResult}></Sidebar>
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
              <ClusterSelection />
            </div>

            <div className="row">
              <div className="col-lg-12">
                <DiagEventsSubscriptionForm
                  clusterStatusResult={this.props.clusterStatusResult}
                  addSubscriptionSubject={this.props.addSubscriptionSubject}
                  addSubscriptionResult={this.props.addSubscriptionResult}
                  listenSubscriptionSubject={this.props.listenSubscriptionSubject}
                  currentCluster={this.props.currentCluster}>
                </DiagEventsSubscriptionForm>
              </div>
            </div>

            <div className="row">
              <div className="col-lg-12">
                <DiagEventsSubscriptionList
                  eventSubscriptions={this.props.eventSubscriptions}
                  deleteSubscriptionSubject={this.props.deleteSubscriptionSubject}
                  deleteResult={this.props.deleteResult}
                  listenSubscriptionSubject={this.props.listenSubscriptionSubject}>
                </DiagEventsSubscriptionList>
              </div>
            </div>

            <div className="row">
              <div className="col-lg-12">
                <DiagEventsList
                  diagnosticEvents={this.props.diagnosticEvents}
                  listenSubscriptionSubject={this.props.listenSubscriptionSubject}>
                </DiagEventsList>
              </div>
            </div>
          </div>
          <!-- /#page-wrapper -->
      </div>
    );
  }

});

export default eventScreen;