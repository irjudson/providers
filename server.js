//var agents = require('./agents'),
var   config = require('./config'),
      controllers = require('./controllers'),
      express = require('express'),
      models = require('./models'),
      mongoose = require('mongoose'),
//    , passport = require('passport')

      app = express(),

      http = require('http'),
      port = process.env.PORT || config.http_port || 3030,
      faye = require('faye')

//    , BearerStrategy = require('passport-http-bearer').Strategy;

var server = app.listen(port);
console.log('listening for http connections on ' + config.base_url);

// Allow cross domain access
app.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', "*");
  res.header('Access-Control-Allow-Credentials', true);
  res.header('Access-Control-Allow-Methods', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');

  // Everything out of magenta is JSON
  res.setHeader('Content-Type', 'application/json');

  // intercept OPTIONS method
  if ('OPTIONS' == req.method) {
    res.send(200);
  } else {
    next();
  }
});

//app.use(passport.initialize());
app.use(express.bodyParser());

// REST endpoint routing

app.get('/api/v1/blobs/:id', controllers.blobs.show);
app.post('/api/v1/blobs', controllers.blobs.create);

app.get('/api/v1/ops/health', controllers.ops.health);

app.get('/api/v1/principals/:id', controllers.principals.show);
app.get('/api/v1/principals', controllers.principals.index);
app.post('/api/v1/principals', controllers.principals.create);

app.get('/api/v1/messages/:id', controllers.messages.show);
app.get('/api/v1/messages', controllers.messages.index);
app.post('/api/v1/messages', controllers.messages.create);

// static serving endpoint

app.use(express.static(__dirname + '/static'));

mongoose.connect(config.mongodb_connection_string);

// Realtime endpoint setup

global.bayeux = new faye.NodeAdapter({
  mount: config.path_prefix + config.realtime_path,
  timeout: 90
});

global.bayeux.bind('handshake', function(clientId) {
  console.log('handshake received: ' + clientId);
});

global.bayeux.bind('subscribe', function(clientId, channel) {
  console.log('subscribe received: ' + clientId + ":" + channel);
});

global.bayeux.bind('publish', function(clientId, channel, data) {
  console.log('publish received: ' + clientId + ":" + channel + " :" + data);
});

global.bayeux.attach(server);
console.log('listening for realtime connections on ' + config.path_prefix + config.realtime_path);

if (process.env.NODE_ENV != "production") {
    mongoose.connection.on('error', function(err) {
        console.error('MongoDB error: %s', err);
    });
}

//services.principals.getServicePrincipal(function(err, callback) {

//  use magenta sdk with system principal to execute agents.
//  var service = new Service(config);

//  service.connect(service.principal, function(err, session) {
//     session.attachAgent(agents.devicePairings);
//  });

//});
