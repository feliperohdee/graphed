const _ = require('lodash');
const rx = require('rxjs');
const rxop = require('rxjs/operators');

const {
    validate,
    validateStore
} = require('./util');

const requiredStoreKeys = [
    'countEdges',
    'deleteEdge',
    'deleteEdges',
    'getEdges',
    'getEdgesByDistance',
    'setEdge'
];

module.exports = class Edge {
    constructor(args = {}) {
        args = validate(args, {
            namespace: true,
            store: true
        });

        const {
            decrementPath = 1 / (10 ** 15),
                defaultDirection,
                defaultEntity,
                namespace,
                store
        } = args;

        if (defaultDirection && (defaultDirection !== 'IN' && defaultDirection !== 'OUT')) {
            throw new Error(`defaultDirection should be "IN" or "OUT", not "${defaultDirection}".`);
        }

        this.decrementPath = decrementPath;
        this.defaultDirection = defaultDirection;
        this.defaultEntity = defaultEntity;
        this.namespace = namespace;
        this.link = this.link.bind(this);

        this.store = validateStore(store, requiredStoreKeys);
        this.countEdges = this.store.countEdges.bind(this.store);
        this.deleteEdge = this.store.deleteEdge.bind(this.store);
        this.deleteEdges = this.store.deleteEdges.bind(this.store);
        this.getAll = this.store.getEdges.bind(this.store);
        this.getAllByDistance = this.store.getEdgesByDistance.bind(this.store);
        this.setEdge = this.store.setEdge.bind(this.store);
    }

    allAll(args = {}) {
        const {
            collection = [],
                direction = this.defaultDirection,
                distance = (collectionSize, fromNodeIndex, toNodeIndex) => {
                    return collectionSize - Math.abs(fromNodeIndex - toNodeIndex);
                },
                entity = this.defaultEntity,
                noTimestamp = false,
                prefix = ''
        } = args;

        if (direction && (direction !== 'IN' && direction !== 'OUT')) {
            return rx.throwError(new Error(`direction should be "IN" or "OUT", not "${direction}".`));
        }

        if (!_.isFunction(distance) && !_.isNumber(distance)) {
            return rx.throwError(new Error('distance should be a number or function with signature "(collectionSize, index): number".'));
        }

        if (!entity) {
            return rx.throwError(new Error('entity is missing.'));
        }

        const collectionSize = _.size(collection);

        return rx.from(collection)
            .pipe(
                rxop.mergeMap((fromNode, fromNodeIndex) => {
                    return rx.from(collection)
                        .pipe(
                            rxop.map((toNode, toNodeIndex) => {
                                if ((direction && fromNodeIndex === toNodeIndex) || (!direction && toNodeIndex <= fromNodeIndex)) {
                                    return null;
                                }

                                return {
                                    direction,
                                    distance: _.isFunction(distance) ? distance(collectionSize, fromNodeIndex, toNodeIndex) : distance,
                                    entity,
                                    fromNode,
                                    noTimestamp,
                                    prefix,
                                    toNode
                                };
                            }),
                            rxop.filter(response => !_.isNull(response))
                        );
                }),
                rxop.mergeMap(this.link)
            );
    }

    count(args = {}) {
        args = _.defaults({}, args, {
            direction: this.defaultDirection,
            entity: this.defaultEntity,
            prefix: ''
        });

        if (args.direction && (args.direction !== 'IN' && args.direction !== 'OUT')) {
            return rx.throwError(new Error(`direction should be "IN" or "OUT", not "${args.direction}".`));
        }

        if (!args.entity) {
            return rx.throwError(new Error('entity is missing.'));
        }

        if (!args.fromNode) {
            return rx.throwError(new Error('fromNode is missing.'));
        }

        return this.countEdges(_.extend({
            namespace: this.namespace + args.prefix
        }, args));
    }

    delete(args = {}) {
        args = _.defaults({}, args, {
            direction: this.defaultDirection,
            entity: this.defaultEntity,
            prefix: ''
        });

        if (args.direction && (args.direction !== 'IN' && args.direction !== 'OUT')) {
            return rx.throwError(new Error(`direction should be "IN" or "OUT", not "${args.direction}".`));
        }

        if (!args.entity) {
            return rx.throwError(new Error('entity is missing.'));
        }

        if (!args.fromNode) {
            return rx.throwError(new Error('fromNode is missing.'));
        }

        if (!args.toNode) {
            return rx.throwError(new Error('toNode is missing.'));
        }

        return this.deleteEdge(_.extend({
            namespace: this.namespace + args.prefix
        }, args));
    }

    deleteByNode(args = {}) {
        args = _.defaults({}, args, {
            direction: this.defaultDirection,
            entity: this.defaultEntity,
            prefix: ''
        });

        if (!args.fromNode) {
            return rx.throwError(new Error('fromNode is missing.'));
        }

        return this.deleteEdges(_.extend({
            namespace: this.namespace + args.prefix
        }, args));
    }

    allByNode(args = {}) {
        args = _.defaults({}, args, {
            direction: this.defaultDirection,
            entity: this.defaultEntity,
            inverse: true,
            onlyNodes: false,
            prefix: ''
        });

        if (!args.fromNode) {
            return rx.throwError(new Error('fromNode is missing.'));
        }

        return this.getAll(_.extend({
                namespace: this.namespace + args.prefix,
                inverse: args.onlyNodes ? false : args.inverse
            }, args))
            .pipe(
                rxop.map(response => {
                    if (args.onlyNodes) {
                        return response.toNode;
                    }

                    return response;
                })
            );
    }

    closest(args = {}) {
        args = _.defaults({}, args, {
            direction: this.defaultDirection || null,
            entity: this.defaultEntity,
            prefix: ''
        });

        if (args.direction && (args.direction !== 'IN' && args.direction !== 'OUT')) {
            return rx.throwError(new Error(`direction should be "IN" or "OUT", not "${args.direction}".`));
        }

        if (!args.entity) {
            return rx.throwError(new Error('entity is missing.'));
        }

        if (!args.fromNode) {
            return rx.throwError(new Error('fromNode is missing.'));
        }

        return this.getAllByDistance(_.extend({
            namespace: this.namespace + args.prefix
        }, args));
    }

    link(args = {}) {
        args = _.defaults({}, args, {
            direction: this.defaultDirection,
            distance: 1,
            entity: this.defaultEntity,
            prefix: ''
        });

        if (args.absoluteDistance) {
            if (!_.isNumber(args.absoluteDistance)) {
                return rx.throwError(new Error(`absoluteDistance should be a number.`));
            }

            if (args.absoluteDistance < 0) {
                return rx.throwError(new Error(`distances should be >= 0.`));
            }
        }

        if (args.direction && (args.direction !== 'IN' && args.direction !== 'OUT')) {
            return rx.throwError(new Error(`direction should be "IN" or "OUT", not "${args.direction}".`));
        }

        if (!_.isUndefined(args.distance) && !_.isNumber(args.distance)) {
            return rx.throwError(new Error('distance should be a number.'));
        }

        if (!args.entity) {
            return rx.throwError(new Error('entity is missing.'));
        }

        if (!args.fromNode) {
            return rx.throwError(new Error('fromNode is missing.'));
        }

        if (!args.toNode) {
            return rx.throwError(new Error('toNode is missing.'));
        }

        return this.setEdge(_.extend({}, args, {
            distance: args.absoluteDistance ? args.absoluteDistance : -(args.distance * this.decrementPath),
            namespace: this.namespace + args.prefix
        }), !args.absoluteDistance);
    }

    traverse(args = {}) {
        args = _.defaults({}, args, {
            concurrency: Number.MAX_SAFE_INTEGER,
            jobs: [],
            maxPath: 30,
            minPath: 2,
            remoteClosest: null,
            remoteClosestIndex: 1
        });

        const initialJob = args.jobs[0];
        const processedEdges = new Set();
        const frequency = {
            all: {}
        };

        if (!initialJob) {
            return rx.of([]);
        }

        const closest = (closestArgs, index = 0) => {
            const operation = (args.remoteClosest && index >= args.remoteClosestIndex) ? args.remoteClosest(closestArgs) : this.closest(closestArgs);

            return operation.pipe(
                rxop.filter(({
                    direction,
                    fromNode,
                    entity,
                    toNode
                }) => {
                    // filter already processed
                    const key = [fromNode, toNode]
                        .sort()
                        .concat(direction)
                        .join(':');

                    const wasProcessed = processedEdges.has(key);

                    if (!wasProcessed) {
                        processedEdges.add(key);

                        if (!direction || direction === 'IN') {
                            frequency.all[fromNode] = frequency.all[fromNode] ? (frequency.all[fromNode] + 1) : 1;
                        }

                        if (!direction || direction === 'OUT') {
                            frequency.all[toNode] = frequency.all[toNode] ? (frequency.all[toNode] + 1) : 1;
                        }

                        if (!frequency[entity]) {
                            frequency[entity] = {};
                        }

                        if (!direction || direction === 'IN') {
                            frequency[entity][fromNode] = frequency[entity][fromNode] ? (frequency[entity][fromNode] + 1) : 1;
                        }

                        if (!direction || direction === 'OUT') {
                            frequency[entity][toNode] = frequency[entity][toNode] ? (frequency[entity][toNode] + 1) : 1;
                        }
                    }

                    return !wasProcessed;
                })
            );
        };

        return closest(initialJob)
            .pipe(
                rxop.toArray(),
                rxop.expand((items, index) => {
                    const nextJob = args.jobs[index + 1];

                    if (nextJob && !_.isEmpty(items)) {
                        return rx.from(items)
                            .pipe(
                                rxop.mergeMap(({
                                    fromNode,
                                    toNode
                                }) => {
                                    return closest(_.extend({}, nextJob, {
                                        fromNode: toNode
                                    }), index + 1);
                                }, null, args.concurrency),
                                rxop.reduce((reduction, items) => {
                                    return reduction.concat(items);
                                }, [])
                            );
                    }

                    return rx.empty();
                }),
                rxop.mergeMap(items => {
                    return rx.from(items);
                }),
                rxop.reduce((reduction, {
                    distance,
                    entity,
                    fromNode,
                    toNode
                }) => {
                    const tails = _.filter(reduction, ({
                        toNode
                    }) => toNode === fromNode);

                    if (_.isEmpty(tails)) {
                        return reduction.concat({
                            distance,
                            fromNode,
                            path: [{
                                node: fromNode
                            }, {
                                distance,
                                entity,
                                node: toNode
                            }],
                            toNode
                        });
                    }

                    return reduction.concat(_.map(tails, tail => {
                        const newDistance = tail.distance + distance;

                        return {
                            distance: newDistance,
                            fromNode: tail.fromNode,
                            path: tail.path.concat({
                                distance,
                                entity,
                                node: toNode
                            }),
                            toNode
                        };
                    }));
                }, []),
                rxop.map(items => _.filter(items, ({
                    path
                }) => {
                    const length = _.size(path);

                    return length >= args.minPath && length <= args.maxPath;
                })),
                rxop.map(items => ({
                    frequency,
                    paths: _.sortBy(items, ['distance'])
                }))
            );
    }
};