### Cassandra Reaper UI

#### Thanks to

Stefan Podkowinski for designing this handy UI for Reaper.
This subdirectory is a fork of the original [cassandra-reaper-ui project](https://github.com/spodkowinski/cassandra-reaper-ui).

#### About

Complimentary web UI for [Cassandra Reaper](https://github.com/thelastpickle/cassandra-reaper).

#### Installation

Instruction to rebuild and embed the UI in Reaper can be found in the root [README.md](https://github.com/thelastpickle/cassandra-reaper/README.md)

#### Development

Getting started to work with the source code is easy. You will need to install

* [node](https://nodejs.org/) (v7.7.0+ recommended)
* [bower](http://bower.io/) (1.8.0+ recommended)

Then run:

```bash
# Assume we are in the reaper project parent directory
$ cd src/ui/
$ npm install
$ bower install
```

The dev-server can be started as follows:

```bash
$ npm run start
```

Afterwards you should be able to access the server under the following url: [http://localhost:8000/webpack-dev-server/](http://localhost:8000/webpack-dev-server/)


Make sure to enable cross-origin requests to the reaper server by starting it with the `-DenableCrossOrigin` jvm parameter.  
Enabling cross-origin requests is not necessary when the UI is embedded in the Reaper server.  

##### Frameworks and tools

It's probably a good idea to familiar yourself with the following set of tools and frameworks, in case you want to know what you're doing while working with the source code.

Used libraries:
* [React](https://facebook.github.io/react/)
* [RxJS](https://github.com/Reactive-Extensions/RxJS)
* [Bootstrap](http://getbootstrap.com/) (CSS)

Tooling:
* [webpack](http://webpack.github.io/)
* [Babel](http://babeljs.io/) (ES6)
* [react-hot-reloader](gaearon.github.io/react-hot-loader/)

##### Building a new release

```bash
$ npm run minimize
```

The content of the `build` directory will correspond to what will be embedded in the Reaper server.
