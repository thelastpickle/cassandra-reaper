import React from "react";
import Table from 'react-bootstrap/lib/Table';
import Stream from 'jsx/stream';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";

const Streams = React.createClass({

    propTypes: {
      endpoint: React.PropTypes.string.isRequired,
      clusterName: React.PropTypes.string.isRequired
    },

    getInitialState() {
      return {streamSessions: [], scheduler: {}};
    },

    componentWillMount: function() {
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
            this.component.setState({streamSessions: data.responseJSON});
        },
        error: function(data) {
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
