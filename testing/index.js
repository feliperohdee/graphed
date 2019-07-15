const Redis = require('smallorange-redis-client');
const {
	RedisStore
} = require('../');

class App {
	constructor() {
		const redis = new Redis({
			connection: {
				port: 6379
			}
		});

		this.store = new RedisStore({
			redis
		});
		this.namespace = 'spec';
	}
}

module.exports = new App();
