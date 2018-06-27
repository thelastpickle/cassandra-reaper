var path = require('path');
var webpack = require('webpack');
var HtmlWebpackPlugin = require('html-webpack-plugin');

var _commonDeps = [
  "bootstrap.css", "sb-admin-2.css", "timeline.css", "font-awesome.css", "metisMenu.css", "style.scss",
  "jquery", "react", "bootstrap", "metisMenu", "sb-admin-2", "rxjs"
];

// include hot reload deps in dev mode
const isDev = process.argv.indexOf('-d') !== -1;
if(isDev) {
  _commonDeps.push("webpack-dev-server/client?http://0.0.0.0:8000"); // WebpackDevServer host and port
  _commonDeps.push("webpack/hot/only-dev-server");
}

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
    deps: _commonDeps
  },
  output: {
    path: path.join(__dirname, 'build'),
    filename: '[name].js'
  },
  resolve: {
    root: [ path.join(__dirname, 'app'), path.join(__dirname, 'bower_components') ],
    alias: {
      "jquery": "jquery/dist/jquery",
      "bootstrap.css": "bootstrap/dist/css/bootstrap.min.css",
      "sb-admin-2.css": "startbootstrap-sb-admin-2/dist/css/sb-admin-2.css",
      "timeline.css": "startbootstrap-sb-admin-2/dist/css/timeline.css",
      "font-awesome.css": "font-awesome/css/font-awesome.min.css",
      "metisMenu.css": "metisMenu/dist/metisMenu.min.css",
      "sb-admin-2": "startbootstrap-sb-admin-2/dist/js/sb-admin-2.js",
      "rxjs": 'rxjs/dist/rx.all',
      "moment": 'moment/moment.js'
    },
    extensions: ['', '.js', '.jsx']
  },
  devtool: "eval",
  resolveLoader: {
    root:  path.join(__dirname, "node_modules")
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
      bbaseUrl: isDev ? '/' : '/webui/'
    }),
    new webpack.ProvidePlugin({
      $: "jquery",
      jQuery: "jquery",
      "windows.jQuery": "jquery"
    }),
    new webpack.HotModuleReplacementPlugin(),
    new webpack.NoErrorsPlugin(),
    new webpack.optimize.CommonsChunkPlugin(/* chunkName= */"deps", /* filename= */"deps.js")
  ],
  module: {
    loaders: [
      {
        test: /\.jsx?$/,
        loaders: ['react-hot', 'babel?modules=amd&optional=runtime'],
        include: path.join(__dirname, 'app')
      },
      { test: /\.css$/, loader: "style-loader!css-loader" },
      { test: /\.scss$/, loaders: ["style","css","resolve-url","sass?sourceMap"]},
      // loaders for font-awesome
      { test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/, loader: "url-loader?limit=10000&minetype=application/font-woff" },
      { test: /\.(gif|ttf|eot|svg?)(\?v=[0-9]\.[0-9]\.[0-9])?$/, loader: 'url-loader?name=[name].[ext]'  }
    ]
  }
};
