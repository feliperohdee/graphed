const _ = require('lodash');
const rx = require('rxjs');
const rxop = require('rxjs/operators');
const {
    Crud,
    Select
} = require('rxjs-dynamodb-client');

const {
    dynamoDbStore,
    validate
} = require('./schema');
const {
    invertDirection,
    pickEdgeData
} = require('./util');

module.exports = class DynamoDBStore extends Crud {
    constructor(args = {}) {
        const {
            value,
            error
        } = dynamoDbStore.constructor.validate(args);

        if (error) {
            throw error;
        }

        super(value.tableName, {
            primaryKeys: {
                partition: 'namespace',
                sort: 'id'
            },
            indexes: {
                distanceIndex: {
                    partition: 'base',
                    sort: 'distance'
                }
            }
        }, {
            dynamoDb: value.dynamoDb
        });

        this.createTable(value.tableName)
            .subscribe(() => null, console.error);
    }

    _composeBase(args = {}, indeterminateDirection = false) {
        let {
            direction,
            entity,
            fromNode,
            namespace
        } = args;

        if (!direction && indeterminateDirection) {
            direction = '~';
        }

        return _.compact([
                namespace,
                fromNode,
                entity,
                direction
            ])
            .join(':');
    }

    _composeId(args = {}, indeterminateDirection = false) {
        let {
            direction,
            entity,
            fromNode,
            toNode
        } = args;

        if (!direction && indeterminateDirection) {
            direction = '~';
        }

        return _.compact([
                fromNode,
                entity,
                direction,
                toNode
            ])
            .join(':');
    }

    _parseId(value) {
        let [
            fromNode,
            entity = null,
            direction = null,
            toNode = null
        ] = _.split(value, ':');

        if (direction === '~') {
            direction = null;
        } else if (direction !== 'IN' && direction !== 'OUT') {
            [toNode, direction] = [direction, null];
        }

        return {
            fromNode,
            entity,
            toNode,
            direction
        };
    }

    countEdges(args = {}) {
        return validate(dynamoDbStore.countEdges, args)
            .pipe(
                rxop.mergeMap(args => {
                    const fromNodeId = this._composeId(args, true);

                    return this.fetch({
                            namespace: args.namespace,
                            id: fromNodeId,
                            select: Select.COUNT
                        })
                        .pipe(
                            rxop.map(response => {
                                return response.stats.count;
                            })
                        );
                })
            );
    }

    deleteEdge(args = {}) {
        return validate(dynamoDbStore.deleteEdge, args)
            .pipe(
                rxop.mergeMap(args => {
                    const {
                        direction,
                        fromNode,
                        namespace,
                        inverse,
                        toNode
                    } = args;

                    const remove = (fromNode, toNode, direction) => {
                        const fromNodeId = this._composeId(_.extend({}, args, {
                            direction,
                            fromNode,
                            toNode
                        }), true);

                        return this.delete({
                                namespace,
                                id: fromNodeId
                            })
                            .pipe(
                                rxop.map(response => {
                                    if (response) {
                                        return pickEdgeData(args, {
                                            direction,
                                            fromNode,
                                            toNode
                                        });
                                    }

                                    return null;
                                })
                            );
                    };

                    if (inverse) {
                        return rx.forkJoin(
                            remove(fromNode, toNode, direction),
                            remove(toNode, fromNode, invertDirection(direction))
                        );
                    }

                    return remove(fromNode, toNode, direction);
                })
            );
    }

    deleteEdges(args = {}) {
        return validate(dynamoDbStore.deleteEdges, args)
            .pipe(
                rxop.mergeMap(args => {
                    return this.getEdges(args)
                        .pipe(
                            rxop.mergeMap(response => {
                                return this.deleteEdge(response);
                            })
                        );
                })
            );
    }

    getEdges(args = {}) {
        return validate(dynamoDbStore.getEdges, args)
            .pipe(
                rxop.mergeMap(args => {
                    return super.fetch(_.extend({}, args, {
                            id: this._composeId(args),
                            prefix: true
                        }))
                        .pipe(
                            rxop.mergeMap(response => {
                                return rx.from(response.data)
                                    .pipe(
                                        rxop.map(response => {
                                            return _.extend({}, response, this._parseId(response.id));
                                        })
                                    );
                            }),
                            rxop.mergeMap(response => {
                                if (args.inverse && args.fromNode) {
                                    return rx.of(response, _.extend({}, response, {
                                            direction: invertDirection(response.direction),
                                            fromNode: response.toNode,
                                            toNode: response.fromNode
                                        }))
                                        .pipe(
                                            rxop.map(response => pickEdgeData(args, response))
                                        );
                                }

                                return rx.of(response)
                                    .pipe(
                                        rxop.map(response => pickEdgeData(args, response))
                                    );
                            })
                        );
                })
            );
    }

    getEdgesByDistance(args = {}) {
        return validate(dynamoDbStore.getEdgesByDistance, args)
            .pipe(
                rxop.mergeMap(args => {
                    const hook = args.distance ? (({
                        request
                    }) => {
                        const [
                            min,
                            max
                        ] = args.distance;

                        request.addPlaceholderName([
                            'distance'
                        ]);

                        if (min && max) {
                            request.addPlaceholderValue({
                                min,
                                max
                            });

                            return ['#partition = :partition AND #distance BETWEEN :min AND :max'];
                        } else if (min) {
                            request.addPlaceholderValue({
                                min
                            });

                            return ['#partition = :partition AND #distance >= :min'];
                        } else {
                            request.addPlaceholderValue({
                                max
                            });

                            return ['#partition = :partition AND #distance <= :max'];
                        }

                    }) : null;

                    return super.fetch({
                            base: this._composeBase(args, true),
                            desc: args.desc,
                            limit: args.limit,
                            indexName: 'distanceIndex',
                            prefix: true
                        }, hook)
                        .pipe(
                            rxop.mergeMap(response => {
                                return rx.from(response.data)
                                    .pipe(
                                        rxop.map(response => {
                                            return _.extend({}, response, this._parseId(response.id));
                                        })
                                    );
                            }),
                            rxop.map(response => {
                                return pickEdgeData(args, response);
                            })
                        );
                })
            );
    }

    setEdge(args = {}) {
        return validate(dynamoDbStore.setEdge, args)
            .pipe(
                rxop.mergeMap(args => {
                    const set = (fromNode, toNode, direction = null) => {
                        args = _.extend({}, args, {
                            fromNode,
                            toNode,
                            direction
                        });

                        return super.insertOrUpdate({}, 'ALL_NEW', ({
                                request
                            }) => {
                                const now = _.now();
                                const base = this._composeBase(args, true);
                                const id = this._composeId(args, true);

                                request.addPlaceholderName([
                                        'base',
                                        'distance',
                                        'createdAt',
                                        'updatedAt'
                                    ])
                                    .addPlaceholderValue({
                                        base,
                                        now
                                    });

                                let expression = '#base = :base, #createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now';

                                if (args.distance > 0) {
                                    request.addPlaceholderValue({
                                        distance: args.distance
                                    });

                                    expression += ', #distance = :distance';
                                } else {
                                    request.addPlaceholderValue({
                                        one: 1,
                                        distance: args.distance
                                    });

                                    expression += ', #distance = if_not_exists(#distance, :one) + :distance';
                                }

                                return [`SET ${expression}`, {
                                    namespace: args.namespace,
                                    id
                                }];
                            })
                            .pipe(
                                rxop.map(response => {
                                    return pickEdgeData(args, {
                                        fromNode,
                                        direction,
                                        distance: response.distance,
                                        toNode
                                    });
                                })
                            );
                    };

                    return rx.forkJoin(
                        set(args.fromNode, args.toNode, args.direction),
                        set(args.toNode, args.fromNode, invertDirection(args.direction))
                    );
                })
            );
    }

    deleteTable(tableName) {
        return this.request.routeCall('deleteTable', {
            TableName: tableName
        });
    }

    createTable(tableName) {
        return this.request.describe()
            .pipe(
                rxop.catchError(() => this.request.routeCall('createTable', {
                    TableName: tableName,
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1
                    },
                    AttributeDefinitions: [{
                        AttributeName: 'base',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'distance',
                        AttributeType: 'N'
                    }, {
                        AttributeName: 'namespace',
                        AttributeType: 'S'
                    }, {
                        AttributeName: 'id',
                        AttributeType: 'S'
                    }],
                    KeySchema: [{
                        AttributeName: 'namespace',
                        KeyType: 'HASH'
                    }, {
                        AttributeName: 'id',
                        KeyType: 'RANGE'
                    }],
                    GlobalSecondaryIndexes: [{
                        IndexName: 'distanceIndex',
                        KeySchema: [{
                            AttributeName: 'base',
                            KeyType: 'HASH'
                        }, {
                            AttributeName: 'distance',
                            KeyType: 'RANGE'
                        }],
                        Projection: {
                            ProjectionType: 'ALL'
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1
                        }
                    }]
                }))
            );
    }
};