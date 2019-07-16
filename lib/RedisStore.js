const _ = require('lodash');
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
            namespace
        } = args;

        return _.compact([
                namespace,
                fromNode,
                entity,
                direction
            ])
            .join(':');
    }

    _parseId(id) {
        const [
            namespace,
            fromNode,
            entity,
            direction = null
        ] = _.split(id, ':');

        return {
            direction,
            entity,
            fromNode,
            namespace
        };
    }

    _getEdgesKeys(args = {}) {
        args = validate(args);

        return this.getKeys(args.namespace, this._composeId(args));
    }

    countEdges(args = {}) {
        args = validate(args, {
            entity: true,
            fromNode: true,
            namespace: true
        });

        const fromNodeId = this._composeId(args);

        return this.countFromSet(fromNodeId);
    }

    deleteEdge(args = {}) {
        args = validate(_.defaults({}, args, {
            direction: null,
            inverse: false
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
                fromNode
            }));

            return this.removeFromSet(fromNodeIdByDistance, toNode)
                .map(response => {
                    if (response) {
                        return pickEdgeData(args, {
                            direction,
                            fromNode,
                            toNode
                        });
                    }

                    return null;
                });
        };

        if (args.inverse) {
            return Observable.forkJoin(
                removeFromSet(fromNode, toNode, direction),
                removeFromSet(toNode, fromNode, invertDirection(direction))
            );
        }

        return removeFromSet(fromNode, toNode, direction);
    }

    deleteEdges(args = {}) {
        args = validate(args);

        return this.getEdges(args)
            .mergeMap(response => {
                return this.deleteEdge(response);
            });
    }

    getEdges(args = {}) {
        args = validate(_.defaults(args, {
            inverse: true
        }));

        return this._getEdgesKeys(args)
            .mergeMap(id => {
                const parsedId = this._parseId(id);

                return this.getRange(id)
                    .map(([
                        toNode,
                        score
                    ]) => _.extend({}, parsedId, {
                        toNode,
                        distance: score
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

        const fromNodeId = this._composeId(args);

        return this.getRangeByScore(fromNodeId, args.distance, args.limit, args.desc)
            .map(([
                toNode,
                distance
            ]) => pickEdgeData(args, {
                distance,
                toNode
            }));
    }

    setEdge(args = {}) {
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
                fromNode
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

    incrementEdge(args = {}) {
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
                fromNode
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