import React from "react";
import Table from 'react-bootstrap/lib/Table';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";

const DroppedMessages = React.createClass({
    propTypes: {
      endpoint: React.PropTypes.string.isRequired,
      clusterName: React.PropTypes.string.isRequired
    },

    getInitialState() {
      return {droppedMessages: [], scheduler: {}};
    },

    componentWillMount: function() {
      this._collectDroppedMessages();
      this.setState({scheduler : setInterval(this._collectDroppedMessages, 10000)});
    },

    componentWillUnmount: function() {
      clearInterval(this.state.scheduler);
    },

    _collectDroppedMessages: function() {    
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname) + '/node/dropped/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpoint),
        method: 'GET',
        component: this,
        dataType: 'json',
        complete: function(data) {
            this.component.setState({droppedMessages: data.responseJSON});
        },
        error: function(data) {
            console.log("Failed getting dropped messages : " + data.responseText);
        }
    })
    },
  
    render: function() {
      const alignRightStyle = {
        textAlign: "right" 
      }

      const headerStyle = {
        fontWeight: "bold" 
      }

      const droppedMessagesHeader = 
      <thead>
        <tr>
          <th>Type</th>
          <th style={alignRightStyle}>Count</th>
          <th style={alignRightStyle}>1 minute</th>
          <th style={alignRightStyle}>5 minutes</th>
          <th style={alignRightStyle}>15 minutes</th>
          <th style={alignRightStyle}>Mean rate</th>
        </tr>
      </thead>
      ;

      const droppedMessagesBody = this.state.droppedMessages.sort((a, b) => {if(a.name < b.name) return -1;
      if(a.name > b.name) return 1;
      return 0;}).map(pool => 
        <tr key={pool.name}>
          <td>{pool.name}</td>
          <td style={alignRightStyle}>{pool.count}</td>
          <td style={alignRightStyle}>{pool.oneMinuteRate}</td>
          <td style={alignRightStyle}>{pool.fiveMinuteRate}</td>
          <td style={alignRightStyle}>{pool.fifteenMinuteRate}</td>
          <td style={alignRightStyle}>{pool.meanRate}</td>
        </tr>
      )
      ;
  
      return (<Table striped bordered condensed hover>
                {droppedMessagesHeader}
                <tbody>
                  {droppedMessagesBody}
                </tbody>
              </Table>
      );
    }
  })

  export default DroppedMessages;