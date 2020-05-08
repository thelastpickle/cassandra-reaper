//
//  Copyright 2018-2019 The Last Pickle Ltd
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
import ReactDOM from "react-dom";

const loginForm = CreateReactClass({

  propTypes: {
    loginSubject: PropTypes.object.isRequired,
    loginResult: PropTypes.object.isRequired,
    loginCallback: PropTypes.func
  },

  getInitialState: function () {
    return {loginResultMsg: null};
  },

  UNSAFE_componentWillMount: function () {
  this._loginResultSubscription = this.props.loginResult.subscribeOnNext(obs =>
      obs.subscribe(
        response => {
          this.setState({loginResultMsg: null});
          window.location.href = "/webui/index.html"
        },
        response => {
            this.setState({loginResultMsg: response.responseText});
        }
      )
    );
  },

  componentWillUnmount: function () {
    this._loginResultSubscription.dispose();
  },

  _onLogin: function (e) {
    const login = {
      username: ReactDOM.findDOMNode(this.refs.in_username).value,
      password: ReactDOM.findDOMNode(this.refs.in_password).value,
      rememberMe: ReactDOM.findDOMNode(this.refs.in_rememberMe).value
    };
    this.props.loginSubject.onNext(login);
  },

  render: function () {
    let loginError = null;
    if (this.state.loginResultMsg) {
      loginError = <div className="alert alert-danger" role="alert">{this.state.loginResultMsg}</div>
    }

    const form = <div className="row">
      <div className="col-lg-12">
        <div className="col-lg-4">&nbsp;</div>
        <div className="col-lg-4"><h2>Cassandra Reaper</h2></div>
        <div className="col-lg-4">&nbsp;</div>
      </div>
      <div className="col-lg-12">
        <div className="col-lg-4">&nbsp;</div>
        <div className="col-lg-4">
          <div className="form-inline">
            <div className="form-group">
              <label htmlFor="in_username">Username:</label>
              <input type="text" className="form-control" ref="in_username" id="in_username"
                    placeholder="Username"></input>
            </div>
            <div className="form-group">
              <label htmlFor="in_password">Password:</label>
              <input type="password" className="form-control" ref="in_password" id="in_password"
                    placeholder="Password"></input>
            </div>
            <br/>
            <div className="form-group">
              <label htmlFor="in_rememberMe">Remember Me</label>
              <input type="checkbox" className="form-control" ref="in_rememberMe" value="true"
                    placeholder="RememberMe"/>
            </div>
            <br/>
            <button type="button" className="btn btn-success" onClick={this._onLogin}>Login</button>
          </div>
        </div>
        <div className="col-lg-4">&nbsp;</div>
      </div>
    </div>


    return (<div className="panel panel-default">
      <div className="panel-body">
        {loginError}
        {form}
      </div>
    </div>);
  }
});

export default loginForm;
