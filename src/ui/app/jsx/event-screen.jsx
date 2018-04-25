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

  getInitialState: function() {
    return {activeSubscription: null, activeEventSource: null};
  },

  componentWillMount: function() {

    this._listenSubscription = this.props.listenSubscriptionSubject.subscribeOnNext(subscription => {

      if(this.state.activeEventSource) {
        this.state.activeEventSource.close();
      }

      const isDev = window != window.top;
      const URL_PREFIX = isDev ? 'http://127.0.0.1:8080' : '';
      const source = new EventSource(`${URL_PREFIX}/diag_event/sse_listen/${subscription.id}`);
      const diagnosticEvents = this.props.diagnosticEvents;
      source.onmessage = function(event) {
        const obj = $.parseJSON(event.data);
        console.debug(`Received diagnostic event (${event.lastEventId})`, obj);
        diagnosticEvents.onNext({eventData: obj, eventId: event.lastEventId});
      };
      source.onerror = function(err) {
        diagnosticEvents.onError(err);
      };
      source.onopen = function() {
        console.debug("EventSource ready for receiving diagnostic events");
      };

      this.setState({activeSubscription: subscription, activeEventSource: source});
    });

    this._unlistenSubscription = this.props.unlistenSubscriptionSubject.subscribeOnNext(subscription => {
      if(this.state.activeSubscription) {
        if(this.state.activeEventSource) {
          this.state.activeEventSource.close();
        }
        this.setState({activeSubscription: null, activeEventSource: null});
      }
    });

    this._deleteSubscription = this.props.deleteSubscriptionSubject.subscribeOnNext(subscription => {
      if(this.state.activeSubscription && this.state.activeSubscription.id == subscription.id) {
        if(this.state.activeEventSource) {
          this.state.activeEventSource.close();
        }
        this.setState({activeSubscription: null, activeEventSource: null});
      }
    });


  },

  componentWillUnmount: function() {
    this._listenSubscription.dispose();
    this._unlistenSubscription.dispose();
    this._deleteSubscription.dispose();
  },

//  componentDidMount: function() {
////    $('#events-selection').multiselect({
//////      enableFiltering: true
////        enableClickableOptGroups: true,
////        onChange: this._onNodeSelection
////    });
//
////    this._subscribe();
//
//  },

  _onCancelView: function() {
    if(this.state.activeSubscription) {
      this.props.unlistenSubscriptionSubject.onNext(this.state.activeSubscription);
    }
  },

//  _onEventSelection: function(option, checked, select) {
//    // TODO
//  },
//
//  _onNodeSelection: function(option, checked, select) {
//    // TODO
//  },


  render: function() {

    var btnLiveView = <button type="button" className="btn btn-sm btn-block btn-default" disabled="disabled">No active subscriptions for live view</button>
    if (this.state.activeSubscription) {
      btnLiveView = <button type="button" className="btn btn-sm btn-block btn-success" onClick={this._onCancelView}>{this.state.activeSubscription.description}</button>
    }

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
                {btnLiveView}
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