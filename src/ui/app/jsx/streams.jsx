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
import Table from 'react-bootstrap/Table';
import Stream from 'jsx/stream';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";

const Streams = CreateReactClass({

    propTypes: {
      endpoint: PropTypes.string.isRequired,
      clusterName: PropTypes.string.isRequired
    },

    getInitialState() {
      return {streamSessions: [], scheduler: {}};
    },

    UNSAFE_componentWillMount: function() {
      this._collectStreams();
      this.setState({scheduler : setInterval(this._collectStreams, 10000)});
    },

    componentWillUnmount: function() {
        clearInterval(this.state.scheduler);
    },

    _collectStreams: function() {
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname)
            + '/node/streams/'
            +  encodeURIComponent(this.props.clusterName)
            + '/'
            + encodeURIComponent(this.props.endpoint),
        method: 'GET',
        component: this,
        dataType: 'json',
        complete: function(data) {
            try {
              this.component.setState({streamSessions: data.responseJSON});
            } catch(error) {
                this.component.setState({streamSessions: []});
            }
        },
        error: function(data) {
            this.component.setState({streamSessions: []});
            console.log("Failed getting streams : " + data.responseText);
        }
    })
    },

    _getIncomingStreams: function(streamSession) {
        const planId = streamSession.planId
        return Object.values(streamSession.streams)
            .filter(stream => stream.sizeToReceive != 0)
            .sort((s1, s2) => s1.peer > s2.peer)
            .map(stream => <Stream planId={planId} direction="incoming" stream={stream} />)
    },

    _getOutgoingStreams: function(streamSession) {
        const planId = streamSession.planId
        return Object.values(streamSession.streams)
            .filter(stream => stream.sizeToSend != 0)
            .sort((s1, s2) => s1.peer > s2.peer)
            .map(stream => <Stream planId={planId} direction="outgoing" stream={stream} />)
    },

    render: function() {

        if (!this.state.streamSessions) {
            console.log("No Streams found");
            return;
        }

        let incomingStreams = this.state.streamSessions
            .map(this._getIncomingStreams)
            // concatenates streams from different sessions into one list
            .reduce((sum, item) => sum.concat(item), [])
        if (!incomingStreams || incomingStreams.length == 0 ) {
            incomingStreams = (<tr><td>There are no incoming streams.</td></tr>)
        }

        let outgoingStreams = this.state.streamSessions
            .map(this._getOutgoingStreams)
            // concatenates streams from different sessions into one list
            .reduce((sum, item) => sum.concat(item), [])
        if (!outgoingStreams || outgoingStreams.length == 0 ) {
            outgoingStreams = (<tr><td>There are no outgoing streams.</td></tr>)
        }

        return (
            <div>
                <div className="col-lg-12">
                    <h4> Incoming Streams </h4>
                    <Table condensed hover>
                        <tbody>
                            {incomingStreams}
                        </tbody>
                    </Table>
                </div>
                <div className="col-lg-12">
                    <h4> Outgoing Streams </h4>
                    <Table condensed hover>
                        <tbody>
                            {outgoingStreams}
                        </tbody>
                    </Table>
                </div>
            </div>
        );
    },

})

export default Streams;
