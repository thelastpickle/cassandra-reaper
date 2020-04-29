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

var path = require('path');
var webpack = require('webpack');
var HtmlWebpackPlugin = require('html-webpack-plugin');

var _commonDeps = [
  "sb-admin-2.css",
  "bootstrap.css",
  "theme.css",
  "timeline.css",
  "font-awesome.css",
  "metisMenu.css",
  "style.scss",
  "jquery",
  "react",
  "bootstrap",
  "metisMenu",
  "sb-admin-2",
  "rxjs",
  "bootstrap-multiselect.css",
  "datatables.net-bs.css",
  "react-datepicker.css"
];

// include hot reload deps in dev mode
const isDev = process.argv.indexOf('-d') !== -1 || process.env.BUILD_DEV;
if (isDev) {
  console.log('Running in development mode!');
  _commonDeps.push("webpack-dev-server/client?http://0.0.0.0:8000"); // WebpackDevServer host and port
  _commonDeps.push("webpack/hot/only-dev-server");
}

const reaperHost = process.env.REAPER_HOST || '127.0.0.1';
console.log(`Reaper remote host is '${reaperHost}'`);

module.exports = {
  entry: {
    index: [
      path.join(__dirname, 'app', 'index.js')
    ],
    schedules: [
      path.join(__dirname, 'app', 'schedules.js')
    ],
    repair: [
      path.join(__dirname, 'app', 'repair.js')
    ],
    snapshot: [
      path.join(__dirname, 'app', 'snapshot.js')
    ],
    segments: [
      path.join(__dirname, 'app', 'segments.js')
    ],
    login: [
      path.join(__dirname, 'app', 'login.js')
    ],
    events: [
      path.join(__dirname, 'app', 'events.js')
    ],
    deps: _commonDeps
  },
  output: {
    path: path.join(__dirname, 'build'),
    filename: '[name].js'
  },
  resolve: {
    modules: [
        path.join(__dirname, 'app'),
        path.join(__dirname, 'bower_components'),
        path.join(__dirname, "node_modules")
    ],
    alias: {
      "jquery": "jquery/dist/jquery",
      "bootstrap.css": "bootstrap.css",
      "theme.css": "bootstrap-theme.css",
      "sb-admin-2.css": "startbootstrap-sb-admin-2/dist/css/sb-admin-2.css",
      "timeline.css": "startbootstrap-sb-admin-2/dist/css/timeline.css",
      "font-awesome.css": "font-awesome/css/font-awesome.min.css",
      "metisMenu.css": "metisMenu/dist/metisMenu.min.css",
      "sb-admin-2": "startbootstrap-sb-admin-2/dist/js/sb-admin-2.js",
      "rxjs": 'rxjs/dist/rx.all',
      "moment": 'moment/moment',
      "datatables.net": "datatables.net/js/jquery.dataTables.js",
      "datatables.net-bs.js": "datatables.net-bs/js/dataTables.bootstrap.js",
      "datatables.net-bs.css": "datatables.net-bs/css/dataTables.bootstrap.css",
      "bootstrap-multiselect.css": "react-boostrap-multiselect/css/bootstrap-multiselect.css",
      "react-datepicker.css": "react-datepicker/dist/react-datepicker.css"
    },
    extensions: ['.js', '.jsx']
  },
  devtool: "eval",
  resolveLoader: {
    modules: [path.join(__dirname, "node_modules")]
  },
  plugins: [
    new HtmlWebpackPlugin({
      filename: 'index.html',
      chunks: ['deps', 'index'],
      hash: true,
      title: ' - Clusters',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head',
      baseUrl: isDev ? '/' : '/webui/'
    }),
    new HtmlWebpackPlugin({ 
      filename: 'repair.html',
      chunks: ['deps', 'repair'],
      hash: true,
      title: ' - Repair',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head',
      baseUrl: isDev ? '/' : '/webui/'
    }),
    new HtmlWebpackPlugin({  
      filename: 'schedules.html',
      chunks: ['deps', 'schedules'],
      hash: true,
      title: ' - Schedules',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head',
      baseUrl: isDev ? '/' : '/webui/'
    }),
    new HtmlWebpackPlugin({
      filename: 'segments.html',
      chunks: ['deps', 'segments'],
      hash: true,
      title: ' - Segments',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head',
      baseUrl: isDev ? '/' : '/webui/'
    }),
    new HtmlWebpackPlugin({
      filename: 'snapshot.html',
      chunks: ['deps', 'snapshot'],
      hash: true,
      title: ' - Snapshots',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head',
      baseUrl: isDev ? '/' : '/webui/'
    }),
    new HtmlWebpackPlugin({ 
      filename: 'login.html',
      chunks: ['deps', 'login'],
      hash: true,
      title: ' - Login',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head',
      baseUrl: isDev ? '/' : '/webui/'
    }),
    new HtmlWebpackPlugin({
      filename: 'events.html',
      chunks: ['deps', 'events'],
      hash: true,
      title: ' - User Auditing',
      template: path.join(__dirname, 'app', 'html_template.ejs'),
      inject: 'head'
    }),
    new webpack.ProvidePlugin({
      $: "jquery",
      jQuery: "jquery",
      "window.jQuery": "jquery"
    }),
    new webpack.HotModuleReplacementPlugin(),
    new webpack.NoEmitOnErrorsPlugin(),
    new webpack.DefinePlugin({
      'GLOBAL_REAPER_HOST': JSON.stringify(reaperHost),
      'GLOBAL_IS_DEV': isDev,
    }),
  ],
  optimization: {
    splitChunks: {
      cacheGroups: {
        deps: {
          filename: "deps.js"
        }
      }
    }
  },
  mode: isDev === "1" ? "development" : "production",
  module: {
    rules:[
      {
        test: /\.(js|jsx)$/,
        exclude: /(node_modules|bower_components)/,
        use: [
          { loader: "react-hot-loader/webpack" },
          {
            loader:"babel-loader",
            options: {
              presets: ["env", "react"]
            }
          }
        ]
      },
      { test: /\.css$/, loader: "style-loader!css-loader" },
      {
        test: /\.scss$/,
        use: [
          { loader: "style-loader"},
          { loader: "css-loader" },
          { loader: "resolve-url-loader" },
          { loader: "sass-loader?sourceMap" }
        ]
      },
      // loaders for font-awesome
      {
        test: /\.(woff|woff2)(\?v=\d+\.\d+\.\d+)?$/,
        use: {
          loader: "url-loader",
          options:{
            mimetype: "application/font-woff",
            name: "./fonts/[name].[ext]",
            publicPath: "../"
          }
        }
      },
      {
        test: /\.(gif|ttf|eot|svg?)(\?v=\d+\.\d+\.\d+)?$/,
        use: {
          loader: "url-loader",
          options: {
            name: "[name].[ext]"
          }
        }
      }
    ]
  }
};
