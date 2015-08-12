var config = {};

config.rabbitmq_servers = ['localhost'];
config.task_finished = 'task_finished';


config.redis = {
    'host': '127.0.0.1',
    'port': 6379,
    'options': {}
}


module.exports = config;
