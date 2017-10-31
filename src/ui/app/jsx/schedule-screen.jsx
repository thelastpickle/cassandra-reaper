import React from "react";
import moment from "moment";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import ScheduleForm from "jsx/schedule-form";
import ScheduleList from "jsx/schedule-list";
import NavBar from "jsx/navbar";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const ScheduleScreen = React.createClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  propTypes: {
    schedules: React.PropTypes.object.isRequired,
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    updateStatusSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    currentCluster: React.PropTypes.string.isRequired,
    statusObservableTimer: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {currentCluster:this.props.currentCluster=="undefined"?"all":this.props.currentCluster};
  },

  changeCurrentCluster : function(clusterName){
    this.setState({currentCluster: clusterName});
  },

  render: function() {

  const navStyle = {
    marginBottom: 0
  };

    return (
    <div id="wrapper">
        <!-- Navigation -->
        <nav className="navbar navbar-default navbar-static-top" role="navigation" style={navStyle}>
            <NavBar></NavBar>
            <!-- /.navbar-header -->

            <Sidebar clusterNames={this.props.clusterNames} currentCluster={this.state.currentCluster}> </Sidebar>
        </nav>

        <div id="page-wrapper">
            <div className="row">
                <ServerStatus statusObservableTimer={this.props.statusObservableTimer}></ServerStatus>
            </div>
            <!-- /.row -->

            <div className="row">
                <div className="col-lg-12">
                    <h1 className="page-header">Schedules</h1>
                </div>
                <!-- /.col-lg-12 -->
            </div>
            <!-- /.row -->

      <div className="row">
                <div className="col-lg-12">
                  <ScheduleForm addScheduleSubject={this.props.addScheduleSubject} addScheduleResult={this.props.addScheduleResult} clusterNames={this.props.clusterNames} currentCluster={this.props.currentCluster}> </ScheduleForm>
                </div>
            </div>

            <div className="row">
                <div className="col-lg-12">
                  <ScheduleList schedules={this.props.schedules}
                              clusterNames={this.props.clusterNames}
                              deleteSubject={this.props.deleteSubject}
                              deleteResult={this.props.deleteResult}
                              updateStatusSubject={this.props.updateStatusSubject}
                              currentCluster={this.state.currentCluster}
                              changeCurrentCluster={this.changeCurrentCluster}> </ScheduleList>
                </div>
            </div>

        </div>
        <!-- /#page-wrapper -->
    </div>
    );
  }

});



export default ScheduleScreen;
