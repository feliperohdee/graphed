const Redis = require('smallorange-redis-client');
const {
	RedisStore
} = require('../');

class App {
	constructor() {
		const redis = new Redis({
			connection: {
				port: 6380
			}
		});

		this.store = new RedisStore({
			redis
		});
		this.namespace = 'graph-1';
	}
}

module.exports = new App();
