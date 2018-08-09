import React from "react";
import Table from 'react-bootstrap/lib/Table';
import {DeleteStatusMessageMixin, humanFileSize, getUrlPrefix, toast} from "jsx/mixin";
import $ from "jquery";

const TpStats = React.createClass({
    propTypes: {
      endpoint: React.PropTypes.string.isRequired,
      clusterName: React.PropTypes.string.isRequired
    },

    getInitialState() {
      return {tpstats: [], scheduler: {}};
    },

    componentWillMount: function() {
      this._collectTpstats();
      this.setState({scheduler : setInterval(this._collectTpstats, 10000)});
    },

    componentWillUnmount: function() {
      clearInterval(this.state.scheduler);
    },

    _collectTpstats: function() {    
      $.ajax({
        url: getUrlPrefix(window.top.location.pathname) + '/node/tpstats/' +  encodeURIComponent(this.props.clusterName) + '/' + encodeURIComponent(this.props.endpoint),
        method: 'GET',
        component: this,
        dataType: 'json',
        complete: function(data) {
            this.component.setState({tpstats: data.responseJSON});
        },
        error: function(data) {
            console.log("Failed getting tpstats : " + data.responseText);
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

      const tpstatsHeader = 
      <thead>
        <tr>
          <th >Pool name</th>
          <th style={alignRightStyle}>Active</th>
          <th style={alignRightStyle}>Pending</th>
          <th style={alignRightStyle}>Completed</th>
          <th style={alignRightStyle}>Blocked</th>
          <th style={alignRightStyle}>All time blocked</th>
        </tr>
      </thead>
      ;

      const tpstatsBody = this.state.tpstats.sort((a, b) => {if(a.name < b.name) return -1;
      if(a.name > b.name) return 1;
      return 0;}).map(pool => 
        <tr key={pool.name} >
          <td >{pool.name}</td>
          <td style={alignRightStyle}>{pool.activeTasks}</td>
          <td style={alignRightStyle}>{pool.pendingTasks}</td>
          <td style={alignRightStyle}>{pool.completedTasks}</td>
          <td style={alignRightStyle}>{pool.currentlyBlockedTasks}</td>
          <td style={alignRightStyle}>{pool.totalBlockedTasks}</td>
        </tr>
      )
      ;
  
      return (<Table striped bordered condensed hover>
                {tpstatsHeader}
                <tbody>
                  {tpstatsBody}
                </tbody>
              </Table>
      );
    }
  })

  export default TpStats;