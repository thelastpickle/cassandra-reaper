//
//  Copyright 2018-2018 Stefan Podkowinski
//  Copyright 2019-2019 The Last Pickle Ltd
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
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import NavBar from "jsx/navbar";
import ClusterSelection from "jsx/cluster-selection";
import DiagEventsSubscriptionForm from "jsx/event-subscription-form";
import DiagEventsSubscriptionList from "jsx/event-subscription-list";
import DiagEventsList from "jsx/event-list";
import {getUrlPrefix} from "jsx/mixin";
import URL_PREFIX from "jsx/uicommon";

const eventScreen = CreateReactClass({

  propTypes: {
    currentCluster: PropTypes.string.isRequired,
    clusterNames: PropTypes.object.isRequired,
    getClusterStatusSubject: PropTypes.object.isRequired,
    clusterSelected: PropTypes.object.isRequired,
    // side-bar
    logoutSubject: PropTypes.object.isRequired,
    logoutResult: PropTypes.object.isRequired,
    // event-subscription-form
    clusterStatusResult: PropTypes.object.isRequired,
    addSubscriptionSubject: PropTypes.object.isRequired,
    addSubscriptionResult: PropTypes.object.isRequired,
    // event-subscription-list
    eventSubscriptions: PropTypes.object.isRequired,
    deleteSubscriptionSubject: PropTypes.object.isRequired,
    deleteResult: PropTypes.object.isRequired,
    // event-list
    diagnosticEvents: PropTypes.object.isRequired,
    listenSubscriptionSubject: PropTypes.object.isRequired,
    switchTheme: PropTypes.func
  },

  getInitialState: function() {
    return {activeSubscription: null, activeEventSource: null};
  },

  componentWillMount: function() {

    this._listenSubscription = this.props.listenSubscriptionSubject.subscribeOnNext(subscription => {

      if(this.state.activeEventSource) {
        this.state.activeEventSource.close();
      }

      const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
      const source = new EventSource(`${URL_PREFIX}/diag_event/sse_listen/${subscription.id}`);
      const diagnosticEvents = this.props.diagnosticEvents;
      source.onmessage = function(event) {
        if(event.name === "ping") {
          return;
        }
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
      if(this.state.activeSubscription && this.state.activeSubscription.id === subscription.id) {
        if(this.state.activeEventSource) {
          this.state.activeEventSource.close();
        }
        this.setState({activeSubscription: null, activeEventSource: null});
      }
    });

    this._clusterSelectedSubscription = this.props.clusterSelected.subscribeOnNext(this._onCancelView);
  },

  componentWillUnmount: function() {
    this._listenSubscription.dispose();
    this._unlistenSubscription.dispose();
    this._deleteSubscription.dispose();
  },

  _onCancelView: function() {
    if(this.state.activeSubscription) {
      this.props.unlistenSubscriptionSubject.onNext(this.state.activeSubscription);
    }
  },

  render: function() {

    var btnLiveView = <button type="button" className="btn btn-sm btn-block btn-default" disabled="disabled">No active subscriptions for live view</button>
    if (this.state.activeSubscription) {
      btnLiveView = <button type="button" className="btn btn-sm btn-block btn-success" onClick={this._onCancelView}>Viewing events for {this.state.activeSubscription.description}</button>
    }

    return (
      <div>
          <nav className="navbar navbar-inverse navbar-static-top" role="navigation">
            <NavBar switchTheme={this.props.switchTheme}></NavBar>

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
            </div>

            <div className="row">
              <ClusterSelection />
            </div>

            <div className="row">
                <div className="col-lg-12 text-right">
                  <small className="text-warning">Cassandra 4.0 or later required for diagnostic events support!</small>
                </div>
            </div>

            <div className="row">
              <div className="col-lg-12">
                <DiagEventsSubscriptionForm
                  clusterStatusResult={this.props.clusterStatusResult}
                  addSubscriptionSubject={this.props.addSubscriptionSubject}
                  addSubscriptionResult={this.props.addSubscriptionResult}
                  listenSubscriptionSubject={this.props.listenSubscriptionSubject}
                  clusterSelected={this.props.clusterSelected}
                  currentCluster={this.props.currentCluster}
                  clusterNames={this.props.clusterNames}>
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
                  clusterSelected={this.props.clusterSelected}
                  listenSubscriptionSubject={this.props.listenSubscriptionSubject}>
                </DiagEventsList>
              </div>
            </div>
          </div>
      </div>
    );
  }

});

export default eventScreen;
