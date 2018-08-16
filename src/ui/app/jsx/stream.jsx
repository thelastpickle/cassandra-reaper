import React from "react";
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import Table from 'react-bootstrap/lib/Table';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import { WithContext as ReactTags } from 'react-tag-input';

const Stream = React.createClass({
    propTypes: {
        planId: React.PropTypes.string.isRequired,
        direction: React.PropTypes.string.isRequired,
        stream: React.PropTypes.object.isRequired
    },

    getInitialState() {
        return {communicating: false, collapsed: true};
    },

    _tableTags: function(tables) {
        const tags = tables.reduce((sum, item) => sum.concat({id: item, text: item}), [])
        return (
            <ReactTags tags={tags} readOnly={true} />
        );
    },

    render: function() {

        const stream = this.props.stream;
        const isActive = stream.completed ? false : true;
        const style = stream.success ? (stream.completed ? "success" : "info") : "danger";
        const state = stream.success ? (stream.completed ? "Done" : "Streaming") : "Error";

        if (this.props.direction == "incoming") {
            var progress = stream.sizeReceived / stream.sizeToReceive * 100;
            p = humanFileSize(stream.sizeReceived, 1024) + " / " + humanFileSize(stream.sizeToReceive, 1024);
            var label = state + " [ " + p + " ]"
            var tables = Object.values(stream.progressReceived).map(tableProgress => tableProgress.table);
            var directionText = "From: ";
        };

        if (this.props.direction == "outgoing") {
            var progress = stream.sizeSent / stream.sizeToSend * 100;
            var p = humanFileSize(stream.sizeSent, 1024) + " / " + humanFileSize(stream.sizeToSend, 1024);
            var label = state + " [ " + p + " ]"
            var tables = Object.values(stream.progressSent).map(tableProgress => tableProgress.table);
            var directionText = "To: ";
        };

        const tableTags = this._tableTags(tables)

        const peerWidth = {
            width: "10%"
        }
        const planWidth = {
            width: "25%"
        }
        const tableWidth = {
            width: "15%"
        }
        const barWidth = {
            width: "50%"
        }

        return (
            <tr>
                <td style={peerWidth}> <strong>{directionText} </strong> {stream.peer} </td>
                <td style={planWidth}> <strong>PlanId: </strong> {this.props.planId} </td>
                <td style={tableWidth}> <strong>Tables: </strong> {tableTags} </td>
                <td style={barWidth}> <ProgressBar now={progress} active={isActive} label={label} bsStyle={style} key={stream.id} /> </td>
            </tr>
        );
    }

})

export default Stream;
