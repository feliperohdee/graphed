const _ = require('lodash');
const DataLoader = require('smallorange-data-loader');
const {
	Observable
} = require('rxjs');

const Base = require('./Base');
const {
	invertDirection,
	pickEdgeData,
	validate
} = require('./util');

module.exports = class RedisStore extends Base {
	constructor(args = {}) {
		super();

		if (!args.redis) {
			throw new Error('redis is missing.');
		}

		this.redis = args.redis;
	}

	_composeId(args = {}) {
		const {
			direction,
			entity,
			fromNode,
			namespace,
			type,
		} = args;

		return _.compact([
				namespace,
				type === 'byDistance' ? 'e.d' : 'e.t',
				fromNode,
				entity,
				direction
			])
			.join(':')
			.replace(':*', '*');
	}

	_parseId(id) {
		const [
			namespace,
			type,
			fromNode,
			entity,
			direction = null
		] = _.split(id, ':');

		return {
			direction,
			entity,
			fromNode,
			namespace,
			type: type === 'e.d' ? 'byDistance' : 'byTimestamp'
		};
	}

	_getEdgesKeys(args = {}) {
		args = validate(_.defaults({}, args, {
			direction: '*'
		}));

		return Observable.merge(
			this._getEdgesKeysByDistance(args),
			this._getEdgesKeysByTimestamp(args)
		);
	}

	_getEdgesKeysByDistance(args = {}) {
		args = validate(_.defaults({
			type: 'byDistance'
		}, args, {
			direction: '*'
		}));

		return this.getKeys(this._composeId(args));
	}

	_getEdgesKeysByTimestamp(args = {}) {
		args = validate(_.defaults({
			type: 'byTimestamp'
		}, args, {
			direction: '*'
		}));

		return this.getKeys(this._composeId(args));
	}

	countEdges(args = {}) {
		args = validate(args, {
			entity: true,
			fromNode: true,
			namespace: true
		});

		const fromNodeId = this._composeId(_.extend({}, args, {
			type: 'byDistance'
		}));

		return this.countFromSet(fromNodeId);
	}

	deleteEdge(args = {}) {
		args = validate(_.defaults({}, args, {
			direction: null
		}), {
			entity: true,
			fromNode: true,
			namespace: true,
			toNode: true,
		});

		const {
			direction,
			fromNode,
			toNode
		} = args;

		const removeFromSet = (fromNode, toNode, direction) => {
			const fromNodeIdByDistance = this._composeId(_.extend({}, args, {
				direction,
				fromNode,
				type: 'byDistance'
			}));

			const fromNodeIdByTimestamp = this._composeId(_.extend({}, args, {
				direction,
				fromNode,
				type: 'byTimestamp'
			}));

			return Observable.forkJoin(
					this.removeFromSet(fromNodeIdByDistance, toNode),
					this.removeFromSet(fromNodeIdByTimestamp, toNode)
				)
				.map(([
					deletedByDistance,
					deletedByTimestamp
				]) => {
					if (deletedByDistance || deletedByTimestamp) {
						return pickEdgeData(args, {
							direction,
							fromNode,
							toNode
						});
					}

					return null;
				});
		};

		return Observable.forkJoin(
			removeFromSet(fromNode, toNode, direction),
			removeFromSet(toNode, fromNode, invertDirection(direction))
		);
	}

	deleteEdges(args = {}) {
		args = validate(args);

		const acc = [];

		return this.getEdges(args)
			.reduce((reduction, response) => {
				const fromNodeId = this._composeId(response);

				acc.push(pickEdgeData(response));

				return reduction.zrem(fromNodeId, response.toNode);
			}, this.redis.multi)
			.mergeMap(this.redis.multiExec)
			.mergeMap(() => Observable.from(acc));
	}

	getEdges(args = {}) {
		args = validate(_.defaults(args, {
			inverse: true
		}));

		let operation;

		if (_.isNil(args.type)) {
			operation = this._getEdgesKeys(args);
		} else {
			operation = args.type === 'byDistance' ? this._getEdgesKeysByDistance(args) : this._getEdgesKeysByTimestamp(args);
		}

		return operation
			.mergeMap(id => {
				const parsedId = this._parseId(id);

				return this.getRange(id)
					.map(toNode => _.extend({}, parsedId, {
						toNode
					}))
					.mergeMap(response => {
						if (args.inverse && args.fromNode) {
							return Observable.of(response, _.extend({}, response, {
								direction: invertDirection(response.direction),
								fromNode: response.toNode,
								toNode: response.fromNode
							}));
						}

						return Observable.of(response);
					})
					.map(response => pickEdgeData(args, response));
			});
	}

	getEdgesByDistance(args = {}) {
		args = validate(args, {
			entity: true,
			fromNode: true,
			namespace: true
		});

		if (!_.isUndefined(args.distance) && !_.isArray(args.distance)) {
			return Observable.throw(new Error(`distance should be an array like [min?: number, max?: number].`));
		}

		if (!_.isUndefined(args.limit) && !_.isArray(args.limit)) {
			return Observable.throw(new Error(`limit should be an array like [offset: number, count: number].`));
		}

		const fromNodeIdByDistance = this._composeId(_.extend({}, args, {
			type: 'byDistance'
		}));

		return this.getRangeByScore(fromNodeIdByDistance, args.distance, args.limit, args.desc)
			.map(([
				toNode,
				distance
			]) => pickEdgeData(args, {
				distance,
				toNode,
				type: 'byDistance'
			}));
	}

	getEdgesByTimestamp(args = {}) {
		args = validate(args, {
			entity: true,
			fromNode: true,
			namespace: true
		});

		if (!_.isUndefined(args.timestamp) && !_.isArray(args.timestamp)) {
			return Observable.throw(new Error(`timestamp should be an array like [min?: number, max?: number].`));
		}

		if (!_.isUndefined(args.limit) && !_.isArray(args.limit)) {
			return Observable.throw(new Error(`limit should be an array like [offset: number, count: number].`));
		}

		const fromNodeIdByDistance = this._composeId(_.extend({}, args, {
			type: 'byDistance'
		}));

		const fromNodeIdByTimestamp = this._composeId(_.extend({}, args, {
			type: 'byTimestamp'
		}));

		return this.getRangeByScore(fromNodeIdByTimestamp, args.timestamp, args.limit, args.desc)
			.mergeMap(([
					toNode,
					timestamp
				]) => this.getScore(fromNodeIdByDistance, toNode)
				.map(distance => pickEdgeData(args, {
					distance,
					timestamp,
					toNode,
					type: 'byTimestamp'
				})));
	}

	setEdgeByDistance(args = {}) {
		args = validate(args, {
			distance: true,
			entity: true,
			fromNode: true,
			namespace: true,
			toNode: true
		});

		const set = (fromNode, toNode, direction = null) => {
			const fromNodeIdByDistance = this._composeId(_.extend({}, args, {
				direction,
				fromNode,
				type: 'byDistance'
			}));

			return this.addToSet(fromNodeIdByDistance, toNode, args.distance)
				.map(() => pickEdgeData(args, {
					fromNode,
					direction,
					toNode
				}));
		}

		return Observable.forkJoin(
			set(args.fromNode, args.toNode, args.direction),
			set(args.toNode, args.fromNode, invertDirection(args.direction))
		);
	}

	setEdgeByTimestamp(args = {}) {
		args = validate(args, {
			entity: true,
			fromNode: true,
			namespace: true,
			timestamp: true,
			toNode: true
		});

		const set = (fromNode, toNode, direction = null) => {
			const fromNodeIdByTimestamp = this._composeId(_.extend({}, args, {
				direction,
				fromNode,
				type: 'byTimestamp'
			}));

			return this.addToSet(fromNodeIdByTimestamp, toNode, args.timestamp)
				.map(() => pickEdgeData(args, {
					fromNode,
					direction,
					toNode
				}));
		}

		return Observable.forkJoin(
			set(args.fromNode, args.toNode, args.direction),
			set(args.toNode, args.fromNode, invertDirection(args.direction))
		);
	}

	incrementEdgeByDistance(args = {}) {
		args = validate(args, {
			distance: true,
			entity: true,
			fromNode: true,
			namespace: true,
			toNode: true
		});

		const increment = (fromNode, toNode, direction = null) => {
			const fromNodeIdByDistance = this._composeId(_.extend({}, args, {
				direction,
				fromNode,
				type: 'byDistance'
			}));

			return this.incrementOnSet(fromNodeIdByDistance, toNode, args.distance, 1)
				.map(distance => pickEdgeData(args, {
					fromNode,
					direction,
					distance,
					toNode
				}));
		}

		return Observable.forkJoin(
			increment(args.fromNode, args.toNode, args.direction),
			increment(args.toNode, args.fromNode, invertDirection(args.direction))
		);
	}
}
