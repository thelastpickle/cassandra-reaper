//
//  Copyright 2015-2016 Stefan Podkowinski
//  Copyright 2016-2019 The Last Pickle Ltd
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

var webpack = require('webpack');
var WebpackDevServer = require('webpack-dev-server');

process.env.BUILD_DEV='1';
var config = require('./webpack.config');

new WebpackDevServer(webpack(config), {
  publicPath: config.output.publicPath,
  contentBase: 'build',
  hot: true,
  historyApiFallback: true
}).listen(8000, 'localhost', function (err, result) {
  if (err) {
    console.log(err);
  }
  console.log('Listening at: localhost:8000');
  console.log('Web App URL:  http://localhost:8000/webpack-dev-server/')
});
