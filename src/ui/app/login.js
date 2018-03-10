import jQuery from "jquery";
import React from "react";
import LoginForm from "jsx/login-form";
import {loginSubject, loginResult} from "observable";


jQuery(document).ready(function($){

  console.info("Login.js this is it");

  React.render(
    React.createElement(LoginForm, {loginSubject, loginResult}),
    document.getElementById('wrapper')
  );

});