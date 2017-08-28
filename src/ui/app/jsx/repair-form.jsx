import React from "react";


const repairForm = React.createClass({

  propTypes: {
    addRepairSubject: React.PropTypes.object.isRequired,
    addRepairResult: React.PropTypes.object.isRequired,
    clusterNames: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {
      addRepairResultMsg: null, clusterNames: [], submitEnabled: false,
      clusterName: this.props.currentCluster!="all"?this.props.currentCluster:this.props.clusterNames[0], keyspace: null, tables: null, owner: null, segments: null,
      parallism: null, intensity: null, cause: null, incrementalRepair: null, formCollapsed: true
    };
  },

  componentWillMount: function() {
    this._repairResultSubscription = this.props.addRepairResult.subscribeOnNext(obs =>
      obs.subscribe(
        r => this.setState({addRepairResultMsg: null}),
        r => this.setState({addRepairResultMsg: r.responseText})
      )
    );

    this._clusterNamesSubscription = this.props.clusterNames.subscribeOnNext(obs =>
      obs.subscribeOnNext(names => {
        this.setState({clusterNames: names});
        if(names.length == 1) this.setState({clusterName: names[0]});
      })
    );
  },

  componentWillUnmount: function() {
    this._repairResultSubscription.dispose();
    this._clusterNamesSubscription.dispose();
  },

  _onAdd: function(e) {
    const repair = {
      clusterName: this.state.clusterName, keyspace: this.state.keyspace,
      owner: this.state.owner
    };
    if(this.state.tables) repair.tables = this.state.tables;
    if(this.state.segments) repair.segmentCount = this.state.segments;
    if(this.state.parallism) repair.repairParallelism = this.state.parallism;
    if(this.state.intensity) repair.intensity = this.state.intensity;
    if(this.state.cause) repair.cause = this.state.cause;
    if(this.state.incrementalRepair) repair.incrementalRepair = this.state.incrementalRepair;

    // Force incremental repair to FALSE if empty
    if(!this.state.incrementalRepair) repair.incrementalRepair = "false";

    this.props.addRepairSubject.onNext(repair);
  },

  _handleChange: function(e) {
    var v = e.target.value;
    var n = e.target.id.substring(3); // strip in_ prefix

    // update state
    const state = this.state;
    state[n] = v;
    this.replaceState(state);

    // validate
    const valid = state.keyspace && state.clusterName && state.owner;
    this.setState({submitEnabled: valid});
  },

  _toggleFormDisplay: function() {
    if(this.state.formCollapsed == true) {
      this.setState({formCollapsed: false});
    }
    else {
      this.setState({formCollapsed: true});
    }
  },

  render: function() {

    let addMsg = null;
    if(this.state.addRepairResultMsg) {
      addMsg = <div className="alert alert-danger" role="alert">{this.state.addRepairResultMsg}</div>
    }

    const clusterItems = this.state.clusterNames.map(name =>
      <option key={name} value={name}>{name}</option>
    );

    const form = <div className="row">
        <div className="col-lg-12">

          <form className="form-horizontal form-condensed">

            <div className="form-group">
              <label htmlFor="in_clusterName" className="col-sm-3 control-label">Cluster*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_clusterName"
                  onChange={this._handleChange} value={this.state.clusterName}>
                  {clusterItems}
                </select>
              </div>
            </div>

            <div className="form-group">
              <label htmlFor="in_keyspace" className="col-sm-3 control-label">Keyspace*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.keyspace}
                  onChange={this._handleChange} id="in_keyspace" placeholder="name of keyspace to repair"/>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_tables" className="col-sm-3 control-label">Tables</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" className="form-control" value={this.state.tables}
                  onChange={this._handleChange} id="in_tables" placeholder="table1, table2, table3"/>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_owner" className="col-sm-3 control-label">Owner*</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" required className="form-control" value={this.state.owner}
                  onChange={this._handleChange} id="in_owner" placeholder="owner name for the repair run (any string)"/>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_segments" className="col-sm-3 control-label">Segment count</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="number" className="form-control" value={this.state.segments}
                  onChange={this._handleChange} id="in_segments" placeholder="amount of segments to create for repair"/>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_parallism" className="col-sm-3 control-label">Parallism</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_parallism"
                  onChange={this._handleChange} value={this.state.parallism}>
                  <option value=""></option>
                  <option value="SEQUENTIAL">Sequential</option>
                  <option value="PARALLEL">Parallel</option>
                  <option value="DATACENTER_AWARE">DC-Aware</option>
                </select>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_intensity" className="col-sm-3 control-label">Repair intensity</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="number" className="form-control" value={this.state.intensity}
                  min="0" max="1"
                  onChange={this._handleChange} id="in_intensity" placeholder="repair intensity"/>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_cause" className="col-sm-3 control-label">Cause</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <input type="text" className="form-control" value={this.state.cause}
                  onChange={this._handleChange} id="in_cause" placeholder="reason repair was started"/>
              </div>
            </div>
            <div className="form-group">
              <label htmlFor="in_incrementalRepair" className="col-sm-3 control-label">Incremental</label>
              <div className="col-sm-9 col-md-7 col-lg-5">
                <select className="form-control" id="in_incrementalRepair"
                  onChange={this._handleChange} value={this.state.incrementalRepair}>
                  <option value="false">false</option>
                  <option value="true">true</option>
                </select>
              </div>
            </div>
            <div className="form-group">
              <div className="col-sm-offset-3 col-sm-9">
                <button type="button" className="btn btn-warning" disabled={!this.state.submitEnabled}
                  onClick={this._onAdd}>Repair</button>
              </div>
            </div>            
          </form>

      </div>
    </div>

    

    let menuDownStyle = {
      display: "inline-block" 
    }

    let menuUpStyle = {
      display: "none" 
    }

    if(this.state.formCollapsed == false) {
      menuDownStyle = {
        display: "none"
      }
      menuUpStyle = {
        display: "inline-block"
      }
    }

    const formHeader = <div className="panel-title" ><a href="#repair-form" data-toggle="collapse" onClick={this._toggleFormDisplay}>Start a new repair</a>&nbsp; <span className="glyphicon glyphicon-menu-down" aria-hidden="true" style={menuDownStyle}></span><span className="glyphicon glyphicon-menu-up" aria-hidden="true" style={menuUpStyle}></span></div>





    return (<div className="panel panel-warning">
              <div className="panel-heading">
                {formHeader}
              </div>
              <div className="panel-body collapse" id="repair-form">
                {addMsg}
                {form}
              </div>
            </div>);
  }
});

export default repairForm;
