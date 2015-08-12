#!/usr/bin/env node

var amqp = require('amqplib/callback_api');
var config = require('./config');
var redisClient = require('./redisClient');
var encoding = require('encoding');


var res = {};


function f(server_index) {
    amqp.connect('amqp://' + config.rabbitmq_servers[server_index], function(err, conn) {
        if (err) {
            console.error("cann't connect to server. try another.")
            if (server_index == config.rabbitmq_servers.length-1) {
                console.error("tried all server but still cann't connect to the server.exit..");
                if (server_index == config.rabbitmq_servers.length - 1) {
                    throw(err);
                }
                else {
                    f(server_index + 1);
                }
            }
        }
        else {
            conn.createChannel(function (err, ch) {
                if (err) {
                    console.log("create channel failed, try another server");
                    if (server_index == config.rabbitmq_servers.length - 1) {
                        throw(err);
                    }
                    else {
                        f(server_index + 1);
                    }
                }
                var queue = config.task_finished;
                ch.assertQueue(queue, {durable: false});
                ch.consume(queue, function(msg) {
                    var receive_msg = msg.content.toString();
                    var receive_obj = JSON.parse(receive_msg)
                    var redis_key = receive_obj.key;
                    redisClient.get_key(redis_key, function (err, reply) {
                        if (err) {
                            console.log(err);
                            throw(err);
                        }
                        res = JSON.parse(reply);
                        reply = null;
                        for(var i in res) {
                            console.log(i);
                        }
                    })
                    //ch.ack(msg);
                }, {noAck: false});
            });
        }
    });
}

function get_res() {
    return res;
}

f(0);

exports.get_res = get_res;
