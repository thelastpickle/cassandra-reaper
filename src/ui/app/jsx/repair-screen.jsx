import React from "react";
import moment from "moment";
import ServerStatus from "jsx/server-status";
import Sidebar from "jsx/sidebar";
import RepairForm from "jsx/repair-form";
import RepairList from "jsx/repair-list";
import NavBar from "jsx/navbar";
import {RowDeleteMixin, RowAbortMixin, StatusUpdateMixin, DeleteStatusMessageMixin, CFsListRender} from "jsx/mixin";

const repairScreen = React.createClass({
  mixins: [RowDeleteMixin, StatusUpdateMixin, RowAbortMixin],

  propTypes: {
    currentCluster: React.PropTypes.string.isRequired,
    addRepairSubject: React.PropTypes.object.isRequired,
    addRepairResult: React.PropTypes.object.isRequired,
    clusterNames: React.PropTypes.object.isRequired,
    deleteSubject: React.PropTypes.object.isRequired,
    deleteResult: React.PropTypes.object.isRequired,
    updateStatusSubject: React.PropTypes.object.isRequired,
    updateIntensitySubject: React.PropTypes.object.isRequired,
    repairs: React.PropTypes.object.isRequired,
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
            <div className="row">
                <div className="col-lg-12">
                    <h1 className="page-header">Repair</h1>
                </div>
                <!-- /.col-lg-12 -->
            </div>
            <!-- /.row -->

            <div className="row">
                <div className="col-lg-12">
                  <RepairForm addRepairSubject={this.props.addRepairSubject} addRepairResult={this.props.addRepairResult} clusterNames={this.props.clusterNames} currentCluster={this.props.currentCluster}> </RepairForm>
                </div>
            </div>
            
            <div className="row">
                <div className="col-lg-12">
                  <RepairList repairs={this.props.repairs}
                              clusterNames={this.props.clusterNames}
                              deleteSubject={this.props.deleteSubject}
                              deleteResult={this.props.deleteResult}
                              updateStatusSubject={this.props.updateStatusSubject}
                              updateIntensitySubject={this.props.updateIntensitySubject}
                              currentCluster={this.state.currentCluster}
                              changeCurrentCluster={this.changeCurrentCluster}> </RepairList>
                </div>
            </div>

        </div>
        <!-- /#page-wrapper -->
    </div>
    );
  }

});



export default repairScreen;
