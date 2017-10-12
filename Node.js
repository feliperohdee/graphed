const _ = require('lodash');
const {
	Observable
} = require('rxjs');

module.exports = class Node {
	constructor(args = {}) {
		const {
			namespace,
			redis
		} = args;

		if (!namespace) {
			throw new Error('namespace is missing.');
		}

		if (!redis) {
			throw new Error('redis is missing.');
		}

		this.redis = redis;
		this.namespace = namespace
	}

	delete(args = {}) {
		const {
			node
		} = args;

		if (!node) {
			return Observable.throw(new Error('node is missing.'));
		}

		return this.redis.hashDel(`${this.namespace}:n`, node)
			.map(response => {
				if (response) {
					return {
						namespace: this.namespace,
						node
					};
				}

				return null;
			});
	}

	get(args = {}) {
		const {
			node
		} = args;

		if (!node) {
			return Observable.throw(new Error('node is missing.'));
		}

		return this.redis.hashGet(`${this.namespace}:n`, node)
			.map(data => ({
				data: JSON.parse(data),
				namespace: this.namespace,
				node
			}))
			.defaultIfEmpty(null);
	}

	multiGet(args = {}) {
		const {
			nodes
		} = args;

		if (!nodes || !_.isArray(nodes)) {
			return Observable.throw(new Error('nodes are missing.'));
		}

		const hashMultiGet = (key, fields) => {
			return this.redis.wrapObservable(this.redis.client.hmget, key, fields);
		};

		return hashMultiGet(`${this.namespace}:n`, nodes)
			.map((data, index) => {
				if (!data) {
					return null;
				}

				return {
					data: JSON.parse(data),
					namespace: this.namespace,
					node: nodes[index]
				};
			})
			.toArray();
	}

	set(args = {}) {
		const {
			node,
			data = {}
		} = args;

		if (!node) {
			return Observable.throw(new Error('node is missing.'));
		}

		return this.redis.hashSet(`${this.namespace}:n`, node, JSON.stringify(data))
			.map(response => {
				return {
					data,
					namespace: this.namespace,
					node
				};
			});
	}

	multiSet(args = {}) {
		const {
			values
		} = args;

		if (!values || !_.isArray(values)) {
			return Observable.throw(new Error('values are missing.'));
		}

		const hashMultiSet = (key, values) => {
			return this.redis.wrapObservable(this.redis.client.hmset, key, values);
		};

		return Observable.from(values)
			.filter(({
				node
			}) => !!node)
			.reduce((reduction, {
				node,
				data
			}) => {
				return reduction.concat(node, JSON.stringify(data || {}));
			}, [])
			.mergeMap(response => {
				return hashMultiSet(`${this.namespace}:n`, response)
					.map(response => {
						if (response === 'OK') {
							return _.map(values, ({
								node,
								data
							}) => ({
								data,
								namespace: this.namespace,
								node
							}));
						}

						return null;
					});
			});
	}

	patch(args = {}) {
		const {
			node,
			data = {}
		} = args;

		if (!node) {
			return Observable.throw(new Error('node is missing.'));
		}

		return this.get({
				node
			})
			.mergeMap(response => {
				if (_.isNull(response)) {
					response = {};
				}

				return this.set({
					data: _.extend(response.data, data),
					node
				});
			});
	}
}
