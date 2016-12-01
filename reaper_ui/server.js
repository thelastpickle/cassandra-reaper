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
  console.log('Listening at localhost:8000');
});