import React from "react";
import moment from "moment";
import {CFsListRender, getUrlPrefix} from "jsx/mixin";
import Button from 'react-bootstrap/lib/Button';
import Modal from 'react-bootstrap/lib/Modal';
import $ from "jquery";
const NotificationSystem = require('react-notification-system');

const SegmentList = React.createClass({
    _notificationSystem: null,

    propTypes: {
      repairRunId: React.PropTypes.string.isRequired
    },
  
    getInitialState: function() {
      const URL_PREFIX = getUrlPrefix(window.top.location.pathname);
      return {segments: null, repairRunId:this.props.repairRunId, scheduler:{}, urlPrefix: URL_PREFIX,
      runningCollapsed: false, doneCollapsed: false, notStartedCollapsed: false};
    },
  
    componentWillMount: function() {
        this._refreshSegments();
        this.setState({scheduler : setInterval(this._refreshSegments, 30000)});
    },
  
    componentWillUnmount: function() {
      clearInterval(this.state.scheduler);
    },

    componentDidMount: function() {
        this._notificationSystem = this.refs.notificationSystem;
    },

    _refreshSegments: function() {
        $.ajax({
            url: this.state.urlPrefix + '/repair_run/' + encodeURIComponent(this.state.repairRunId) + '/segments',
            method: 'GET',
            component: this,
            complete: function(data) {
              this.component.setState({segments: $.parseJSON(data.responseText)});
            }
        });
    },

    _toggleRunningDisplay: function() {
        if(this.state.runningCollapsed === true) {
          this.setState({runningCollapsed: false});
        }
        else {
          this.setState({runningCollapsed: true});
        }
    },
    
    _toggleDoneDisplay: function() {
        if(this.state.doneCollapsed === true) {
          this.setState({doneCollapsed: false});
        }
        else {
          this.setState({doneCollapsed: true});
        }
    },
  
    _toggleNotStartedDisplay: function() {
        if(this.state.notStartedCollapsed === true) {
          this.setState({notStartedCollapsed: false});
        }
        else {
          this.setState({notStartedCollapsed: true});
        }
    },

    _toast: function(message, type, uid) {
        event.preventDefault();
        this._notificationSystem.addNotification({
        message: message,
        level: type,
        autoDismiss: 3
    });
    },

    render: function() {
        let rowsRunning = null;
        let rowsNotStarted = null;
        let rowsDone = null;
        let runningSegments = [];
        let notStartedSegments = [];
        let doneSegments = [];

        function compareByStartDate(a, b) {
            let comparison = 0;
            if (a.startTime > b.startTime) {
                comparison = 1;
            } else if (a.startTime < b.startTime) {
                comparison = -1;
            }
            return comparison * -1;
        }

        function compareByEndDate(a, b) {
            let comparison = 0;
            if (a.endTime > b.endTime) {
                comparison = 1;
            } else if (a.endTime < b.endTime) {
                comparison = -1;
            }
            return comparison * -1;
        }

        function compareByStartToken(a, b) {
            let comparison = 0;
            if (a.tokenRange.start > b.tokenRange.start) {
                comparison = 1;
            } else if (a.tokenRange.start < b.tokenRange.start) {
                comparison = -1;
            }
            return comparison;
        }

      if(this.state.segments !== null) {
          runningSegments = this.state.segments.filter(segment => segment.state === 'RUNNING');
          rowsRunning = runningSegments.sort(compareByStartDate).map(segment =>
              <tbody key={segment.id+'-rows'}>
              <Segment segment={segment} key={segment.id+'-head'} urlPrefix={this.state.urlPrefix} refreshSegments={this._refreshSegments} notify={this._toast}/>
              </tbody>
          );

          notStartedSegments = this.state.segments.filter(segment => segment.state === 'NOT_STARTED');
          rowsNotStarted = notStartedSegments.sort(compareByStartToken).map(segment =>
              <tbody key={segment.id+'-rows'}>
              <Segment segment={segment} key={segment.id+'-head'}/>
              </tbody>
          );

          doneSegments = this.state.segments.filter(segment => segment.state === 'DONE');
          rowsDone = doneSegments.sort(compareByEndDate).map(segment =>
              <tbody key={segment.id+'-rows'}>
              <Segment segment={segment} key={segment.id+'-head'} urlPrefix={this.state.urlPrefix} refreshSegments={this._refreshSegments} notify={this._toast}/>
              </tbody>
          );
      }

      let tableRunning = null;
      if(rowsRunning === null) {
        tableRunning = <div className="clusterLoader"></div>;
      } else if(rowsRunning.length === 0) {
        tableRunning = <div className="alert alert-info" role="alert">No running segments found</div>
      } else {
        tableRunning = <div className="row">
            <div className="col-sm-12">
                <div className="table-responsive">
                    <table className="table table-bordered table-hover table-striped">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Start token</th>
                                <th>End token</th>
                                <th>Fail count</th>
                                <th>State</th>
                                <th>Host</th>
                                <th>Started</th>
                                <th></th>
                            </tr>
                        </thead>
                        {rowsRunning}
                    </table>
                </div>
            </div>
        </div>;
      }
  
      let tableDone = null;
      if(rowsDone === null) {
        tableDone = <div className="clusterLoader"></div>;
      } else if(rowsDone.length === 0) {
        tableDone = <div className="alert alert-info" role="alert">No segment done yet</div>
      } else {
        tableDone = <div className="row">
            <div className="col-sm-12">
                <div className="table-responsive">
                    <table className="table table-bordered table-hover table-striped">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Start token</th>
                                <th>End token</th>
                                <th>Fail count</th>
                                <th>State</th>
                                <th>Host</th>
                                <th>Started</th>
                                <th>Ended</th>
                                <th>Duration</th>
                                <th></th>
                            </tr>
                        </thead>
                        {rowsDone}
                    </table>
                </div>
            </div>
        </div>;
      }

      let tableNotStarted = null;
      if(rowsNotStarted === null) {
          tableNotStarted = <div className="clusterLoader"></div>;
      } else if(rowsNotStarted.length === 0) {
        tableNotStarted = <div className="alert alert-info" role="alert">No more segment to process</div>
      } else {
        tableNotStarted = <div className="row">
            <div className="col-sm-12">
                <div className="table-responsive">
                    <table className="table table-bordered table-hover table-striped">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Start token</th>
                                <th>End token</th>
                                <th>Fail count</th>
                                <th>State</th>
                            </tr>
                        </thead>
                        {rowsNotStarted}
                    </table>
                </div>
            </div>
        </div>;
      }
  
      let menuRunningDownStyle = {
        display: "none" 
      };
  
      let menuRunningUpStyle = {
        display: "inline-block" 
      };
  
      if(this.state.runningCollapsed === true) {
        menuRunningDownStyle = {
          display: "inline-block"
        };
        menuRunningUpStyle = {
          display: "none"
        }
      }
  
  
      let menuDoneDownStyle = {
        display: "inline-block" 
      };
  
      let menuDoneUpStyle = {
        display: "none" 
      };
  
      if(this.state.doneCollapsed === true) {
        menuDoneDownStyle = {
          display: "none"
        };
        menuDoneUpStyle = {
          display: "inline-block"
        }
      }
  
      let menuNotStartedDownStyle = {
        display: "none" 
      };
  
      let menuNotStartedUpStyle = {
        display: "inline-block" 
      };
  
      if(this.state.notStartedCollapsed === true) {
        menuNotStartedDownStyle = {
          display: "inline-block"
        };
        menuNotStartedUpStyle = {
          display: "none"
        }
      }

      const runningHeader = <div className="panel-title"><a href="#segments-running" data-toggle="collapse" onClick={this._toggleRunningDisplay}>Running ({runningSegments.length ? runningSegments.length : '-'})&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuRunningDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuRunningUpStyle}></span></a></div>;
      const doneHeader = <div className="panel-title"><a href="#segments-done" data-toggle="collapse" onClick={this._toggleDoneDisplay}>Done ({doneSegments.length ? doneSegments.length : '-'})&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDoneDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuDoneUpStyle}></span></a></div>;
      const notStartedHeader = <div className="panel-title"><a href="#segments-notstarted" data-toggle="collapse" onClick={this._toggleNotStartedDisplay}>Not started ({notStartedSegments.length ? notStartedSegments.length : '-'})&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuNotStartedDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuNotStartedUpStyle}></span></a></div>;

      return (
          <div>
            <NotificationSystem ref="notificationSystem" />
            <div className="panel panel-primary">
            <div className="panel-heading">
                {runningHeader}
            </div>
            <div className="panel-body collapse in" id="segments-running">
                {tableRunning}
            </div>
            </div>
            <div className="panel panel-success">
            <div className="panel-heading">
                {doneHeader}
            </div>
            <div className="panel-body collapse in" id="segments-done">
                {tableDone}
            </div>
            </div>
            <div className="panel panel-info">
            <div className="panel-heading">
                {notStartedHeader}
            </div>
            <div className="panel-body collapse in" id="segments-notstarted">
                {tableNotStarted}
            </div>
            </div>
          </div>
      );
    }
  });

const Segment = React.createClass({
    propTypes: {
        segment: React.PropTypes.object.isRequired,
        refreshSegments: React.PropTypes.func,
        notify: React.PropTypes.func
    },
    
    getInitialState: function() {
        return {segment: {}};
    },

    _abortSegment: function() {
        console.log("Aborting segment " + this.props.segment.id);
        this.props.notify("Aborting segment " + this.props.segment.id, "warning", this.props.segment.id);
        $.ajax({
            url: this.props.urlPrefix + '/repair_run/' + encodeURIComponent(this.props.segment.runId) + '/segments/abort/' + encodeURIComponent(this.props.segment.id),
            method: 'GET',
            component: this,
            success: function(data) {
                this.component.props.notify("Successfully aborted segment " + this.component.props.segment.id, "success", this.component.props.segment.id)
            },
            complete: function(data) {
                this.component.props.refreshSegments();
            },
            error: function(data) {
                this.component.props.notify("Failed aborting segment " + this.component.props.segment.id  + " : " + data.responseText, "error", this.component.props.segment.id)
            }
        });
    },

    render: function() {
        if (this.props.segment.state === 'NOT_STARTED') {
            return  <tr>
                <td>{this.props.segment.id}</td>
                <td>{this.props.segment.tokenRange.baseRange.start}</td>
                <td>{this.props.segment.tokenRange.baseRange.end}</td>
                <td>{this.props.segment.failCount}</td>
                <td><Button bsStyle='primary'>{this.props.segment.state}</Button></td>
            </tr>
        } else if (this.props.segment.state === 'RUNNING') {
            return  <tr>
                <td>{this.props.segment.id}</td>
                <td>{this.props.segment.tokenRange.baseRange.start}</td>
                <td>{this.props.segment.tokenRange.baseRange.end}</td>
                <td>{this.props.segment.failCount}</td>
                <td><Button bsStyle='warning'>{this.props.segment.state}</Button></td>
                <td>{this.props.segment.coordinatorHost}</td>
                <td>{moment(this.props.segment.startTime).format("LLL")}</td>
                <td><Button bsStyle='danger' onClick={() => this._abortSegment()}>Abort</Button></td>
            </tr>
        } else {
            return  <tr>
                <td>{this.props.segment.id}</td>
                <td>{this.props.segment.tokenRange.baseRange.start}</td>
                <td>{this.props.segment.tokenRange.baseRange.end}</td>
                <td>{this.props.segment.failCount}</td>
                <td><Button bsStyle='success'>{this.props.segment.state}</Button></td>
                <td>{this.props.segment.coordinatorHost}</td>
                <td>{moment(this.props.segment.startTime).format("LLL")}</td>
                <td>{moment(this.props.segment.endTime).format("LLL")}</td>
                <td>{moment.duration(moment(this.props.segment.endTime).diff(moment(this.props.segment.startTime))).humanize()}</td>
                <td><Button bsStyle='danger' onClick={() => this._abortSegment()}>Replay</Button></td>
            </tr>
        }
    }
});

export default SegmentList;