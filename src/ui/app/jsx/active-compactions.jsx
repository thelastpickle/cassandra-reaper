import React from "react";
import Table from 'react-bootstrap/lib/Table';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import ProgressBar from 'react-bootstrap/lib/ProgressBar';
import $ from "jquery";

const ActiveCompactions = React.createClass({
    propTypes: {
      endpoint: React.PropTypes.string.isRequired,
      clusterName: React.PropTypes.string.isRequired
    },

    getInitialState() {
      return {activeCompactions: [], scheduler: {}};
    },

    componentWillMount: function() {
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
            this.component.setState({activeCompactions: data.responseJSON});
        },
        error: function(data) {
            console.log("Failed getting active compactions : " + data.responseText);
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
  
      return (<div className="col-lg-12"> 
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