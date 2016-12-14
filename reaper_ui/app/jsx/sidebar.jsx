import React from "react";

const sidebar = React.createClass({

  render: function() {

    return (
      <div className="navbar-default sidebar" role="navigation">
          <div className="sidebar-nav navbar-collapse">
              <ul className="nav" id="side-menu">
                  <li>
                      <a href="index.html"><i className="fa fa-sitemap fa-fw"></i> Cluster</a>
                  </li>
                  <li className="active">
                      <a href="schedules.html"><i className="fa fa-calendar fa-fw"></i> Schedules</a>
                  </li>
                  <li>
                      <a href="repair.html"><i className="fa fa-wrench fa-fw"></i> Repair</a>
                  </li>

              </ul>
          </div>
      </div>
    );
  }
});

export default sidebar;