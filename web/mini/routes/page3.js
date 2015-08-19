var express = require('express');
var router = express.Router();
var receive = require('../receiver');
var redisClient = require('../redisClient');
var convert_table = require('./convert');

/* GET home page. */

router.get('/city(/all)?', function (req, res, next) {
    redisClient.get('result', function(err, reply) {
        if (err) {
            console.log(err)
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            reply = null;
            var flow = result['source'];
            result = null;
            if (typeof flow === 'undefined') {
                res.type('application/json');
                res.json({});
            }
            else {
                var flow_point = [];
                var flow_data_json = [];
                for (var src_city in flow) {
                    if (flow[city]['total'] < 10) {
                        continue;
                    }
                    flow[src_city]['top10'].forEach(function(item, index) {
                        flow_point.push([{name: src_city}, {name: item[1]}]);
                        if (typeof flow_data_json[item[1]] === 'undefined') {
                            flow_data_json[item[1]] = item[0];
                        }
                        else {
                            flow_data_json[item[1]] += item[0];
                        }
                    });
                }
                var flow_data = [];
                for (var city in flow_data_json) {
                    flow_data.push({
                        name: city, value: flow_data_json[city]});
                }
            }
            result = {data: flow_point, value: flow_data};
            res.type('application/json');
            res.json(result);
        }
    });
});

router.param('city', function(req, res, next, city) {
    redisClient.get('result', function (err, reply) {
        if (err) {
            console.log(err);
            next(err);
        }
        else {
            var city_chinese = convert_table[city];
            if (typeof city_chinese === 'undefined') {
                city_chinese = city;
            }
            var result = JSON.parse(reply);
            var city_source = result['source'][city_chinese];
            result = null;
            reply = null;
            if (typeof city_source === 'undefined') {
                res.type('application/json');
                res.json({});
            }
            else {
                var flow_point = [];
                var flow_data = [];
                city_source['top10'].forEach(function(item) {
                    flow_point.push([{name: city_chinese}, {name: item[1]}]);
                    flow_data.push({name: item[1], value: item[0]});
                });
                var flow = {data: flow_point, value: flow_data};
                res.type('application/json');
                res.json(flow);
            }
        }
    });
});

router.get('/city/:city', function (req, res, next) {
    next.end();
});

module.exports = router;
