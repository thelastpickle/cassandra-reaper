import React from "react";

const sidebar = React.createClass({
  propTypes: {
    currentCluster: React.PropTypes.string.isRequired,
    clusterNames: React.PropTypes.object    
  },

  render: function() {
    const sideBarStyle = {marginTop: 0};
    return (
      <div className="navbar-default sidebar" style={sideBarStyle} role="navigation">
          <div className="sidebar-nav navbar-collapse">
              <ul className="nav" id="side-menu">
                  <li>
                      <a href={'index.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-sitemap fa-fw"></i> Cluster</a>
                  </li>
                  <li className="active">
                      <a href={'schedules.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-calendar fa-fw"></i> Schedules</a>
                  </li>
                  <li>
                      <a href={'repair.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-wrench fa-fw"></i> Repair</a>
                  </li>
                  <li>
                      <a href={'snapshot.html?currentCluster=' + this.props.currentCluster}><i className="fa fa-camera fa-fw"></i> Snapshot</a>
                  </li>
              </ul>
          </div>
      </div>
    );
  }
});

export default sidebar;