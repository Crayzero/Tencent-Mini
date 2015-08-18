var express = require('express');
var router = express.Router();
var receive = require('../receiver');
var redisClient = require('../redisClient');
var convert_table = require('./convert');
var locations = require('./location');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/prov(/all)?', function (req, res, next) {
    var result = null;
    redisClient.get('result', function (err, reply) {
        if (err) {
            console.log(err);
            next(err);
        }
        else {
            var result = JSON.parse(reply);
            reply = null;
            var whole_country = {};
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
            res.type('application/json');
            res.json(whole_res);
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
        console.log(prov_chinese);
        if (typeof reply_json[prov_chinese] === 'undefined') {
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
                    console.log(city);
                    continue;
                }
                citys_data.push({name: city,
                        value: citys[city]['count'],
                        geoCoord:[locations[city].lnt, locations[city].lat],
                            });
            }
            top10_data = top_10.map(function(video) {
                return {
                            value: video[0],
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


module.exports = router;
