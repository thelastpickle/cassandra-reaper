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
import Table from 'react-bootstrap/lib/Table';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import $ from "jquery";

const ActiveCompactions = CreateReactClass({
    propTypes: {
      endpoint: PropTypes.string.isRequired,
      clusterName: PropTypes.string.isRequired
    },

    getInitialState() {
      return {pendingCompactions: 0, activeCompactions: [], scheduler: {}};
    },

    UNSAFE_componentWillMount: function() {
      this._listActiveCompactions();
      this.setState({scheduler : setInterval(this._listActiveCompactions, 1000)});
    },

    componentWillUnmount: function() {
      clearInterval(this.state.scheduler);
    },

    _listActiveCompactions: function() {    
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname) + '/node/compactions/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpoint),
        method: 'GET',
        component: this,
        dataType: 'json',
        complete: function(data) {
          try {
            this.component.setState({
                pendingCompactions: data.responseJSON.pendingCompactions,
                activeCompactions: data.responseJSON.activeCompactions
            });
          } catch(error) {
            this.component.setState({pendingCompactions:-1, activeCompactions: []});
            console.error('Error occurred while retrieving compaction stats:', error);
          }
        },
        error: function(data) {
            console.log("Failed getting active compactions : " + data.responseText);
            this.component.setState({activeCompactions: []});
        }
    })
    },
  
    render: function() {

      function roundValue(number, roundTo) {
        return number==null?"":number.toFixed(roundTo);
      }

      const alignRightStyle = {
        textAlign: "right" 
      }

      const headerStyle = {
        fontWeight: "bold" 
      }

      const compactionsHeader = 
        <thead>
          <tr>
            <th>Id</th>
            <th>Type</th>
            <th>Keyspace</th>
            <th>Table</th>
            <th style={alignRightStyle}>Size</th>
            <th>Progress</th>
          </tr>
        </thead>
      ;

      const activeCompactionsBody = this.state.activeCompactions.sort((a, b) => {if(a.id < b.id) return -1;
        if(a.id > b.id) return 1;
        return 0;}).map(compaction => 
          <tr>
            <td>{compaction.id}</td>
            <td>{compaction.type}</td>
            <td>{compaction.keyspace}</td>
            <td>{compaction.table}</td>
            <td style={alignRightStyle}>{humanFileSize(compaction.total, 1024)}</td>
            <td>
              <ProgressBar now={(compaction.progress*100)/compaction.total} active bsStyle="success" 
                                   label={(roundValue((compaction.progress*100)/compaction.total,2)) + '%'}
                                   key={compaction.id}/>
            </td>
          </tr>
        )
      ;

      const pendingCompactionsCnt = this.state.pendingCompactions;
  
      return (<div className="col-lg-12">
              <div> Pending Tasks: {pendingCompactionsCnt} </div>
              <Table striped bordered condensed hover>
                {compactionsHeader}
                <tbody>
                  {activeCompactionsBody}
                </tbody>
              </Table>
              </div>
      );
    }
  })

  export default ActiveCompactions;
