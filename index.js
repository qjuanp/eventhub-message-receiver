var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
const crypto = require('crypto');
app.get('/', function(req, res){
  res.sendFile(__dirname + '/index.html');
});

io.on('connection', function(socket){
  console.log('a user connected');
  socket.on('disconnect', function(){
    console.log('user disconnected');
  });
});

//================================
// EventHub Management test - takes in a JSON settings file
// containing settings for connecting to the Hub:
// - protocol: should never be set, defaults to amqps
// - SASKeyName: name of your SAS key which should allow send/receive
// - SASKey: actual SAS key value
// - serviceBusHost: name of the host without suffix (e.g. https://foobar-ns.servicebus.windows.net/foobar-hub => foobar-ns)
// - eventHubName: name of the hub (e.g. https://foobar-ns.servicebus.windows.net/foobar-hub => foobar-hub)
//
// Connects to the $management hub of the service bus, and sends a request to read properties for the given event hub
// Dumps out the number of partitions, their IDs, and then quits.
//================================

'use strict';
var AMQPClient = require('amqp10').Client,
  moment = require('moment'),
  Policy = require('amqp10').Policy,
  Promise = require('bluebird');

function create_sas_key(uri, key_name, key)
{
    // Token expires in one hour
    var expiry = moment().add(1, 'day').unix();

    var string_to_sign = encodeURIComponent(uri) + '\n' + expiry;
    var hmac = crypto.createHmac('sha256', key);
    hmac.update(string_to_sign);
    var signature = hmac.digest('base64');
    var token = 'SharedAccessSignature sr=' + encodeURIComponent(uri) + '&sig=' + encodeURIComponent(signature) + '&se=' + expiry + '&skn=' + key_name;

    return token;
}

var settings = {
    serviceBusHost: "cloudweek",
    eventHubName: "uniandes",
    partitions: 4,
    SASKeyName: "send",
    SASKey:"UQvsUaY2U6gvURspPgkN2MQ3XQUkd09Tllolv/Cvnbw=",
  };



var protocol = settings.protocol || 'amqps';
var serviceBusHost = settings.serviceBusHost + '.servicebus.windows.net/';
if (settings.serviceBusHost.indexOf(".") !== -1) {
  serviceBusHost = settings.serviceBusHost;
}


console.log("Uri to sign", protocol +'://'+serviceBusHost);

var sasName = settings.SASKeyName;
var sasKey = settings.SASKey;
var eventHubName = settings.eventHubName;
var numPartitions = settings.partitions;

var uri = protocol + '://' + encodeURIComponent(sasName) + ':' + encodeURIComponent(sasKey) + '@' + serviceBusHost ;

var recvAddr = eventHubName + '/ConsumerGroups/$default/Partitions/';

var msgVal = Math.floor(Math.random() * 1000000);

console.log("URI", uri);


var client = new AMQPClient(Policy.EventHub);
var errorHandler = function(myIdx, rx_err) { console.warn('==> RX ERROR: ', rx_err); };
var messageHandler = function (myIdx, msg) {
  console.log('received(' + myIdx + '): ', msg.body);
  if (msg.annotations) console.log('annotations: ', msg.annotations);
  io.emit('event message', msg.body);
  if (msg.body.DataValue === msgVal) {
    client.disconnect().then(function () {
      console.log('disconnected, when we saw the value we inserted.');
      process.exit(0);
    });
  }
};

function range(begin, end) {
  return Array.apply(null, new Array(end - begin)).map(function(_, i) { return i + begin; });
}

var createPartitionReceiver = function(curIdx, curRcvAddr) {
  return client.createReceiver(curRcvAddr)
    .then(function (receiver) {
      receiver.on('message', messageHandler.bind(null, curIdx));
      receiver.on('errorReceived', errorHandler.bind(null, curIdx));
    });
};

client.connect(uri)
  .then(function () {
    return Promise.all([
      Promise.map(range(0, numPartitions), function(idx) {
        return createPartitionReceiver(idx, recvAddr + idx);
      })
    ]);
  })
  .error(function (e) {
    console.warn('connection error: ', e);
  });

http.listen(process.env.PORT || 3001, function(){
  console.log('listening on *:3001');
});