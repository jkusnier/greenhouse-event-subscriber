"use strict"

var spark = require('spark');
var JSON5 = require('json5');
var extend = require('xtend');

var mongo = require('mongoskin');
var db = mongo.db("mongodb://localhost:27017/greenhouse", {native_parser:true});

var config = require('./config.json');

spark.on('login', function() {

    var chunks = [];
    var appendToQueue = function(arr) {
        for(var i=0;i<arr.length;i++) {
            var line = (arr[i] || "").trim();
            if (line == "") {
                continue;
            }
            chunks.push(line);
            if (line.indexOf("data:") == 0) {
                processItem(chunks);
                chunks = [];
            }
        }
    };

    var processItem = function(arr) {
        var obj = {};
        for(var i=0;i<arr.length;i++) {
            var line = arr[i];

            if (line.indexOf("event:") == 0) {
                obj.name = line.replace("event:", "").trim();
            } else if (line.indexOf("data:") == 0) {
                line = line.replace("data:", "");
                obj = extend(obj, JSON.parse(line));
            }
        }

        var sdata = JSON5.parse(obj.data);

        if (obj.name === "temperature") {
            console.log(JSON.stringify(obj));
            console.log(obj.data);
            var dbObj = {
                name: obj.name,
                humidity: sdata.humidity,
                fahrenheit: sdata.fahrenheit,
                celsius: sdata.celsius,
                coreid: obj.coreid,
                published_at: obj.published_at,
                created_on: new Date()
            };
            console.log(dbObj);

            db.collection('environment').insert(
                {
                    name: obj.name,
                    humidity: sdata.humidity,
                    fahrenheit: sdata.fahrenheit,
                    celsius: sdata.celsius,
                    coreid: obj.coreid,
                    published_at: obj.published_at,
                    created_on: new Date()
                }, function(err, result) {
                    if (err) throw err;
                    if (result) console.log('Added!');
                });
        };
    }

    var onData = function(event) {
        var chunk = event.toString();
        if (chunk.indexOf("data:") != 0) {
            chunk = chunk.replace("data:", "\ndata:");
        }
        appendToQueue(chunk.split("\n"));
    };

    var getEventStream = function(coreId) {
        console.log("Starting Stream");
        var requestObj = spark.getEventStream('temperature', coreId, onData);
        requestObj.on('end', function() {
            console.log("Stream End");
            requestObj.removeAllListeners();
            requestObj.destroy();
            getEventStream(coreId);
        });
    };

    var coreIds = config.cores;
    coreIds.forEach(function(coreId){
        getEventStream(coreId);
    });

    return 0;

});

spark.login(config.sparkLogin);
