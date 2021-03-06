var express = require('express');
var router = express.Router();
var redisClient = require('../redisClient');
var convert_table = require('./convert');
var locations = require('./location');

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
                var flow_point_json = {};
                var flow_data_json = {};
                var citys = ['北京市', '上海市', '广州市', '深圳市',
                    '武汉市', '成都市']
                citys.forEach(function (src_city, index) {
                    flow[src_city]['top10'].forEach(function(item, index) {
                        if (index > 10) {
                            return ;
                        }
                        flow_point.push([{name: src_city}, {
                                            name: item[1],
                                            value: item[0]}
                        ]);
                        if (typeof flow_data_json[src_city] === 'undefined') {
                            flow_data_json[src_city] = item[0];
                        }
                        else {
                            flow_data_json[src_city] += item[0];
                        }
                    });
                    flow_point_json[src_city] = flow_point;
                    flow_point = [];
                });

                /*
                var flow_data = [];
                for (var city in flow_data_json) {
                    flow_data.push({
                        name: city, value: flow_data_json[city]});
                }
                */
            }
            result = {data: flow_point_json, value: flow_data_json};
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

            var citys = [];
            for (var src_city in locations) {
                if (locations[src_city].prov == city_chinese) {
                    citys.push(src_city);
                }
            }

            reply = null;
            var flow_point = [];
            var flow_data = [];
            var flow_data_json = {};
            var city_flow_point = {};

            citys.forEach(function (src_city)  {
                var city_source = result['source'][src_city];
                if (typeof city_source === 'undefined') {
                    return ;
                }
                flow_data_json[src_city] = 0;

                city_source['top10'].forEach(function(item) {
                    if (typeof locations[item[1]] === 'undefined') {
                        return ;
                    }
                    if (locations[item[1]] && locations[item[1]].prov != city_chinese) {
                        return ;
                    }
                    flow_point.push([{name: src_city}, {
                                        name: item[1],
                                        value: item[0]
                        }]
                    );
                    flow_data_json[src_city] += item[0];
                });
                city_flow_point[src_city] = flow_point;
                flow_point = [];
            });

            /*
            for (var src_city in flow_data_json) {
                flow_data.push({
                    name: src_city,
                    value: flow_data_json[src_city]
                });
            }
            */

            var flow = {data: city_flow_point, value: flow_data_json};
            res.type('application/json');
            res.json(flow);
        }
    });
});

router.get('/city/:city', function (req, res, next) {
    //next.end();
});

module.exports = router;
