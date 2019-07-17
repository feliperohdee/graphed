const _ = require('lodash');
const {
    Observable
} = require('rxjs');
const {
    Crud,
    Select
} = require('smallorange-dynamodb-client');

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

    _composeId(args = {}) {
        const {
            direction,
            entity,
            fromNode,
            toNode
        } = args;

        return _.compact([
                fromNode,
                entity,
                direction,
                toNode
            ])
            .join(':');
    }

    _composeBase(args) {
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

    _parseId(value) {
        let [
            fromNode,
            entity = null,
            direction = null,
            toNode = null
        ] = _.split(value, ':');

        if (direction !== 'IN' && direction !== 'OUT') {
            [toNode, direction] = [direction, null];
        }

        return {
            fromNode,
            entity,
            toNode,
            direction
        };
    };

    countEdges(args = {}) {
        args = validate(args, {
            entity: true,
            fromNode: true,
            namespace: true
        });

        const fromNodeId = this._composeId(args);

        return this.fetch({
                namespace: args.namespace,
                id: fromNodeId,
                select: Select.COUNT
            })
            .map(response => {
                return response.stats.count;
            });
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

        const remove = (fromNode, toNode, direction) => {
            const fromNodeId = this._composeId(_.extend({}, args, {
                direction,
                fromNode,
                toNode
            }));

            return this.delete({
                    namespace: args.namespace,
                    id: fromNodeId
                })
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
                remove(fromNode, toNode, direction),
                remove(toNode, fromNode, invertDirection(direction))
            );
        }

        return remove(fromNode, toNode, direction);
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

        return this.fetch(_.extend({}, args, {
                id: this._composeId(args)
            }))
            .mergeMap(response => {
                return Observable.from(response.data)
                    .map(response => {
                        return _.extend({}, response, this._parseId(response.id));
                    });
            })
            .mergeMap(response => {
                if (args.inverse && args.fromNode) {
                    return Observable.of(response, _.extend({}, response, {
                            direction: invertDirection(response.direction),
                            fromNode: response.toNode,
                            toNode: response.fromNode
                        }))
                        .map(response => pickEdgeData(args, response));
                }

                return Observable.of(response)
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

        return this.fetch({
                base: this._composeBase(args),
                indexName: 'distanceIndex'
            })
            .mergeMap(response => {
                return Observable.from(response.data)
                    .map(response => {
                        return _.extend({}, response, this._parseId(response.id));
                    });
            })
            .map(response => pickEdgeData(args, response));
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
                    const base = this._composeBase(args);
                    const id = this._composeId(args);

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
                            zero: 0,
                            distance: args.distance
                        });

                        expression += ', #distance = if_not_exists(#distance, :zero) + :distance';
                    }

                    return [`SET ${expression}`, {
                        namespace: args.namespace,
                        id
                    }]
                })
                .map(response => pickEdgeData(args, {
                    fromNode,
                    direction,
                    distance: response.distance,
                    toNode
                }));
        }

        return Observable.forkJoin(
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
            .catch(() => this.request
                .routeCall('createTable', {
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
                }));
    }
}