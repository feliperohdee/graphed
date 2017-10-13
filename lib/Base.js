const {
	Observable
} = require('rxjs');

module.exports = class Base {
	getKeys(pattern = '*') {
		return this.redis.wrapObservable(this.redis.client.keys, pattern);
	}

	getRange(fromNodeId, start = 0, stop = -1) {
		return this.redis.wrapObservable(this.redis.client.zrange, fromNodeId, start, stop);
	}

	getRangeByScore(fromNodeId, range = [], limit = [], desc = false) {
		let [
			min = '-inf',
			max = '+inf'
		] = range;

		let [
			offset = 0,
			count = 50
		] = limit;

		if(desc){
			[min, max] = [max, min];
		}

		return this.redis.wrapObservable(desc ? this.redis.client.zrevrangebyscore : this.redis.client.zrangebyscore, fromNodeId, min, max, 'WITHSCORES', 'LIMIT', offset, count)
			.map((value, index) => {
				const isIndex = index % 2;

				return isIndex ? parseFloat(value) : value;
			})
			.bufferCount(2);
	}

	getScore(fromNodeId, toNode) {
		return this.redis.wrapObservable(this.redis.client.zscore, fromNodeId, toNode)
			.map(parseFloat);
	}

	countFromSet(fromNodeId, min = '-inf', max = '+inf'){
		return this.redis.wrapObservable(this.redis.client.zcount, fromNodeId, min, max)
			.defaultIfEmpty(0);
	}

	addToSet(fromNodeId, toNode, value) {
		return this.redis.wrapObservable(this.redis.client.zadd, fromNodeId, value, toNode);
	}

	removeFromSet(fromNodeId, ...keys) {
		return this.redis.wrapObservable(this.redis.client.zrem, fromNodeId, ...keys);
	}

	incrementOnSet(fromNodeId, toNode, value, initial = 1) {
		return this.redis.wrapObservable(this.redis.client.zincrby, fromNodeId, value, toNode)
			.map(parseFloat)
			.mergeMap(response => {
				const isInitial = response !== 0 && response === value;

				if(isInitial) {
					return this.addToSet(fromNodeId, toNode, initial + value)
						.mapTo(initial + value);
				}

				return Observable.of(response);
			});
	}
}
