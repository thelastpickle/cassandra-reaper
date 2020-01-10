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
import Tooltip from 'react-bootstrap/Tooltip';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";

const ClientRequestLatency = CreateReactClass({
    propTypes: {
      endpoint: PropTypes.string.isRequired,
      clusterName: PropTypes.string.isRequired
    },

    getInitialState() {
      return {clientRequestLatencies: [], scheduler: {}};
    },

    UNSAFE_componentWillMount: function() {
      this._collectClientRequestLatencies();
      this.setState({scheduler : setInterval(this._collectclientRequestLatencies, 10000)});
    },

    componentWillUnmount: function() {
      clearInterval(this.state.scheduler);
    },

    _collectClientRequestLatencies: function() {    
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname) + '/node/clientRequestLatencies/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpoint),
        method: 'GET',
        component: this,
        dataType: 'json',
        complete: function(data) {
          try {
            this.component.setState({clientRequestLatencies: data.responseJSON});
          } catch(error) {
            this.component.setState({clientRequestLatencies: []});
          }
        },
        error: function(data) {
          this.component.setState({clientRequestLatencies: []});
          console.log("Failed getting client request latencies : " + data.responseText);
        }
    })
    },
  
    render: function() {
      function roundLatency(number, roundTo) {
        return number==null?"":number.toFixed(roundTo);
      }

      const alignRightStyle = {
        textAlign: "right" 
      }

      const headerStyle = {
        fontWeight: "bold" 
      }

      const tooltipP50 = (
        <Tooltip id="tooltip">
          50th Percentile
        </Tooltip>
      );

      const tooltipP75 = (
        <Tooltip id="tooltip">
          75th Percentile
        </Tooltip>
      );

      const tooltipP95 = (
        <Tooltip id="tooltip">
          95th Percentile
        </Tooltip>
      );

      const tooltipP99 = (
        <Tooltip id="tooltip">
          99th Percentile
        </Tooltip>
      );

      const tooltipP999 = (
        <Tooltip id="tooltip">
          99.9th Percentile
        </Tooltip>
      );

      const tooltipMax = (
        <Tooltip id="tooltip">
          Max
        </Tooltip>
      );

      const tooltip1min = (
        <Tooltip id="tooltip">
          1 minute rate
        </Tooltip>
      );

      const tooltip5min = (
        <Tooltip id="tooltip">
          5 minutes rate
        </Tooltip>
      );

      const tooltip15min = (
        <Tooltip id="tooltip">
          15 minutes rate
        </Tooltip>
      );

      const clientRequestLatenciesHeader = 
        <thead>
          <tr>
            <th>Type</th>
            <th style={alignRightStyle}>p50</th>
            <th style={alignRightStyle}>p75</th>
            <th style={alignRightStyle}>p95</th>
            <th style={alignRightStyle}>p99</th>
            <th style={alignRightStyle}>p999</th>
            <th style={alignRightStyle}>Max</th>
            <th style={alignRightStyle}>1min</th>
            <th style={alignRightStyle}>5min</th>
            <th style={alignRightStyle}>15min</th>
          </tr>
        </thead>
        ;

      const clientRequestLatenciesBodyFirstPart = this.state.clientRequestLatencies.sort(
        (a, b) => {if(a.name + a.type < b.name + b.type) return -1;
        if(a.name + a.type > b.name + b.type) return 1;
        return 0;})
        .filter(metric => metric.type == "Latency")
        .map(clientRequest => 
          <tr key={clientRequest.name}>
            <td>{clientRequest.name}</td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP50}><span>{roundLatency(clientRequest.p50, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP75}><span>{roundLatency(clientRequest.p75, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP95}><span>{roundLatency(clientRequest.p95, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP99}><span>{roundLatency(clientRequest.p99, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP999}><span>{roundLatency(clientRequest.p999, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipMax}><span>{roundLatency(clientRequest.max, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltip1min}><span>{roundLatency(clientRequest.oneMinuteRate, 2)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltip5min}><span>{roundLatency(clientRequest.fiveMinuteRate, 2)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltip15min}><span>{roundLatency(clientRequest.fifteenMinuteRate, 2)}</span></OverlayTrigger></td>
          </tr>
      )
      ;

      const clientRequestLatenciesBodySecondPart = this.state.clientRequestLatencies.sort(
        (a, b) => {if(a.name + a.type < b.name + b.type) return -1;
        if(a.name + a.type > b.name + b.type) return 1;
        return 0;})
        .filter(metric => metric.type != "Latency")
        .map(clientRequest => 
          <tr key={clientRequest.name + clientRequest.type}>
            <td >{clientRequest.name} {clientRequest.type}</td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP50}><span>{roundLatency(clientRequest.p50, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP75}><span>{roundLatency(clientRequest.p75, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP95}><span>{roundLatency(clientRequest.p95, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP99}><span>{roundLatency(clientRequest.p99, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipP999}><span>{roundLatency(clientRequest.p999, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltipMax}><span>{roundLatency(clientRequest.max, 3)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltip1min}><span>{roundLatency(clientRequest.oneMinuteRate, 2)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltip5min}><span>{roundLatency(clientRequest.fiveMinuteRate, 2)}</span></OverlayTrigger></td>
            <td style={alignRightStyle}><OverlayTrigger placement="top" overlay={tooltip15min}><span>{roundLatency(clientRequest.fifteenMinuteRate, 2)}</span></OverlayTrigger></td>
          </tr>
      )
      ;
  
      return (<div className="col-lg-12"> 
              <Table striped bordered condensed hover>
                {clientRequestLatenciesHeader}
                <tbody>
                  {clientRequestLatenciesBodyFirstPart}
                  {clientRequestLatenciesBodySecondPart}
                </tbody>
              </Table>
              </div>
      );
    }
  })

  export default ClientRequestLatency;
