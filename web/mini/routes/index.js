var express = require('express');
var router = express.Router();
var receive = require('../receiver');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/prov', function (req, res, next) {
    var result = null;
    result = receive.get_res();
    res.type('application/json');
    res.json(result);
});

module.exports = router;
