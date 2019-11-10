const _ = require('lodash');
const rx = require('rxjs');
const rxop = require('rxjs/operators');
const {
    Crud,
    Select
} = require('rxjs-dynamodb-client');

const {
    invertDirection,
    pickEdgeData,
    validate
} = require('./util');

module.exports = class DynamoDBStore extends Crud {
    constructor(args = {}) {
        if (!args.dynamoDb) {
            throw new Error('noDynamoDbError');
        }

        if (!args.tableName) {
            throw new Error('noTableNameError');
        }

        super(args.tableName, {
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
            dynamoDb: args.dynamoDb
        });

        this.tableName = args.tableName;
        this.createTable()
            .subscribe(() => null, console.error);
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
        args = validate(args, {
            entity: true,
            fromNode: true,
            namespace: true
        });

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
    }

    deleteEdge(args = {}) {
        args = validate(_.defaults({}, args, {
            direction: null,
            inverse: false
        }), {
            entity: true,
            fromNode: true,
            namespace: true,
            toNode: true
        });

        const {
            direction,
            fromNode,
            toNode
        } = args;

        const remove = (fromNode, toNode, direction) => {
            const fromNodeId = this._composeId(_.extend({}, args, {
                direction,
                fromNode,
                toNode
            }), true);

            return this.delete({
                    namespace: args.namespace,
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

        if (args.inverse) {
            return rx.forkJoin(
                remove(fromNode, toNode, direction),
                remove(toNode, fromNode, invertDirection(direction))
            );
        }

        return remove(fromNode, toNode, direction);
    }

    deleteEdges(args = {}) {
        args = validate(args);

        return this.getEdges(args)
            .pipe(
                rxop.mergeMap(response => {
                    return this.deleteEdge(response);
                })
            );
    }

    getEdges(args = {}) {
        args = validate(_.defaults(args, {
            inverse: true
        }));

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
    }

    getEdgesByDistance(args = {}) {
        args = validate(args, {
            entity: true,
            fromNode: true,
            namespace: true
        });

        if (!_.isUndefined(args.distance) && !_.isArray(args.distance)) {
            return rx.throwError(new Error(`distance should be an array like [min?: number, max?: number].`));
        }

        if (!_.isUndefined(args.limit) && !_.isNumber(args.limit)) {
            return rx.throwError(new Error(`limit should be an number.`));
        }

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
    }

    deleteTable() {
        return this.request.routeCall('deleteTable', {
            TableName: this.tableName
        });
    }

    createTable() {
        return this.request.describe()
            .pipe(
                rxop.catchError(() => this.request.routeCall('createTable', {
                    TableName: this.tableName,
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