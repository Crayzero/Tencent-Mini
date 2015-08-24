var express = require('express');
var router = express.Router();
var receive = require('../receiver');
var redisClient = require('../redisClient');
var convert_table = require('./convert');
var locations = require('./location');
var Heap = require('heap');

/* GET home page. */

router.get('/prov(/all)?', function (req, res, next) {
    var result = null;
    redisClient.get('result', function (err, reply) {
        if (err) {
            console.log(err);
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            console.log(result);
            reply = null;
            var whole_country = {};
            if (result == null) {
                res.type('application/json');
                res.json({});
                res.end();
            }
            else {
                var top10 = result.top10.name;
                for (var i in result) {
                    if (i == 'count_per_five_second') {
                        continue;
                    }
                    else if (i == 'source') {
                        continue;
                    }
                    else if (i == 'time') {
                        continue;
                    }
                    else if (i == 'top10') {
                        continue;
                    }
                    else if (i == 'extra') {
                        continue;
                    }
                    for (var city in result[i]['city']) {
                        if (city == '未知') {
                            continue;
                        }
                        if (typeof whole_country[city] === 'undefined') {
                            whole_country[city] = result[i]['city'][city]['count'];
                        }
                        else {
                            whole_country[city] += result[i]['city'][city]['count'];
                        }
                    }
                }
                result = null;
                result = {};
                result['top10'] = top10;
                var whole_res = []
                for(var city in whole_country) {
                    if (typeof locations[city] === 'undefined') {
                        continue;
                    }
                    whole_res.push({name: city,
                                value: whole_country[city],
                                geoCoord:[locations[city].lnt, locations[city].lat],
                            });
                }
                result['citys'] = whole_res;
                res.type('application/json');
                res.json(result);
            }
        }
    });
});

router.param('prov', function (req, res, next, prov) {
    redisClient.get('result', function (err, reply) {
        if (err) {
            next(err);
        }
        var reply_json = JSON.parse(reply);
        reply = null;
        var prov_chinese = convert_table[prov];
        if (typeof prov_chinese === 'undefined') {
            prov_chinese = prov;
        }
        if (reply_json == null || typeof reply_json[prov_chinese] === 'undefined') {
            res.type('application/json');
            res.json({});
        }
        else {
            var citys = reply_json[prov_chinese]['city'];
            var top_10 = reply_json[prov_chinese]['top10'];
            var citys_data = [];
            var top10_data = [];
            for (var city in citys) {
                if (typeof locations[city] === 'undefined') {
                    continue;
                }
                if (locations[city].prov != prov_chinese) {
                    continue;
                }
                citys_data.push({name: city,
                        value: citys[city]['count'],
                        geoCoord:[locations[city].lnt, locations[city].lat],
                            });
            }
            top10_data = top_10.map(function(video) {
                return {
                            value: video[0][0],
                            name: video[1][0],
                            };
            });
            var prov_res = {};
            prov_res['city'] = citys_data;
            prov_res['top10'] = top10_data;
            res.type('application/json');
            res.json(prov_res);
        }
    });
});

router.get('/prov/:prov', function (req, res, next) {
    res.end();
});

router.get('/distribute/:vid', function (req, res, next) {
    res.end();
});

router.param('vid', function(req, res, next, vid) {
    redisClient.get('result', function(err, reply) {
        if (err) {
            console.log(err);
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            var distribute = result.top10.distribute;
            var vid_distribute = distribute[vid];
            reply = null;
            result = null;
            var ret_heap = new Heap(function (a,b ) {
                return a.value - b.value;
            });

            for (var city in vid_distribute) {
                if (ret_heap.size() < 10) {
                    ret_heap.push({
                        'name': city,
                        'value': vid_distribute[city]
                        //'geoCoord':[locations[city].lnt, locations[city].lat]
                    });
                }
                else {
                    if (vid_distribute[city] > ret_heap.peek().value) {
                        ret_heap.replace({
                            'name': city,
                            'value': vid_distribute[city]
                        });
                    }
                }
            }
            res.type('json');
            res.json(ret_heap.toArray());
        }
    });
});

module.exports = router;
