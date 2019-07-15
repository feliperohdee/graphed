const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const chaiSubset = require('chai-subset');
const {
    Observable
} = require('rxjs');

const app = require('../testing');
const {
    RedisStore
} = require('../');

chai.use(chaiSubset);
chai.use(sinonChai);

const expect = chai.expect;
const store = app.store;

describe('RedisStore.js', () => {
    beforeEach(done => {
        Observable.forkJoin(
                store.setEdge({
                    distance: 1,
                    entity: 'entity',
                    fromNode: '0',
                    namespace: app.namespace,
                    toNode: '1'
                }),
                store.setEdge({
                    distance: 1,
                    entity: 'entity',
                    fromNode: '0',
                    namespace: app.namespace,
                    toNode: '2'
                }),
                store.setEdge({
                    distance: 1,
                    direction: 'OUT',
                    entity: 'entity-2',
                    fromNode: '1',
                    namespace: app.namespace,
                    toNode: '2'
                })
            )
            .subscribe(null, null, done);
    });

    afterEach(done => {
        Observable.forkJoin(
                store.deleteEdges({
                    fromNode: '0',
                    namespace: app.namespace
                })
                .toArray(),
                store.deleteEdges({
                    fromNode: '1',
                    namespace: app.namespace
                })
                .toArray(),
                store.deleteEdges({
                    fromNode: '2',
                    namespace: app.namespace
                })
                .toArray()
            )
            .subscribe(null, null, done);
    });

    describe('constructor', () => {
        it('should throw if no redis provided', () => {
            expect(() => new RedisStore({})).to.throw('redis is missing.');
        });

        it('should have redis', () => {
            expect(store.redis).to.be.an('object');
        });
    });

    describe('_composeId', () => {
        it('should compose id with keys', () => {
            expect(store._composeId({
                namespace: app.namespace
            })).to.equal('spec');

            expect(store._composeId({
                fromNode: 'fromNode',
                namespace: app.namespace
            })).to.equal('spec:fromNode');

            expect(store._composeId({
                entity: 'entity',
                fromNode: 'fromNode',
                namespace: app.namespace
            })).to.equal('spec:fromNode:entity');

            expect(store._composeId({
                direction: 'direction',
                entity: 'entity',
                fromNode: 'fromNode',
                namespace: app.namespace
            })).to.equal('spec:fromNode:entity:direction');
        });
    });

    describe('_parseId', () => {
        it('should parse id', () => {
            expect(store._parseId('spec:fromNode')).to.deep.equal({
                direction: null,
                entity: undefined,
                fromNode: 'fromNode',
                namespace: 'spec'
            });
        });
    });

    describe('_getEdgesKeys', () => {
        it('should throw if invalid', () => {
            expect(() => store._getEdgesKeys()).to.throw('namespace is missing or wrong.');
        });

        it('should get edge keys', done => {
            store._getEdgesKeys({
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(5);
                    expect(response).to.deep.include('spec:0:entity');
                    expect(response).to.deep.include('spec:1:entity-2:OUT');
                    expect(response).to.deep.include('spec:1:entity');
                    expect(response).to.deep.include('spec:2:entity-2:IN');
                    expect(response).to.deep.include('spec:2:entity');
                }, null, done);
        });

        it('should get edge keys by fromNode', done => {
            store._getEdgesKeys({
                    fromNode: '1',
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(2);
                    expect(response).to.deep.include('spec:1:entity-2:OUT');
                    expect(response).to.deep.include('spec:1:entity');
                }, null, done);
        });

        it('should get edge keys by fromNode and entity', done => {
            store._getEdgesKeys({
                    fromNode: '1',
                    entity: 'entity-2',
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(1);
                    expect(response).to.deep.include('spec:1:entity-2:OUT');
                }, null, done);
        });

        it('should get edge keys by fromNode, entity and direction', done => {
            store._getEdgesKeys({
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT',
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(1);
                    expect(response).to.deep.include('spec:1:entity-2:OUT');
                }, null, done);
        });
    });

    describe('countEdges', () => {
        it('should throw if invalid', () => {
            expect(() => store.countEdges()).to.throw('entity, fromNode, namespace are missing or wrong.');
        });

        it('should return count', done => {
            store.countEdges({
                    namespace: app.namespace,
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT'
                })
                .subscribe(response => {
                    expect(response).to.equal(1);
                }, null, done);
        });
    });

    describe('deleteEdge', () => {
        it('should throw if invalid', () => {
            expect(() => store.deleteEdge()).to.throw('entity, fromNode, namespace, toNode are missing or wrong.');
        });

        it('should return deleted edge', done => {
            store.deleteEdge({
                    namespace: app.namespace,
                    fromNode: '0',
                    entity: 'entity',
                    toNode: '1'
                })
                .subscribe(response => {
                    expect(response).to.deep.equal({
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    });
                }, null, done);
        });

        it('should return deleted edges', done => {
            store.deleteEdge({
                    namespace: app.namespace,
                    fromNode: '0',
                    entity: 'entity',
                    inverse: true,
                    toNode: '1'
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }, {
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);
                }, null, done);
        });

        it('should return null if no edges delete', done => {
            store.deleteEdge({
                    namespace: app.namespace,
                    fromNode: '0',
                    entity: 'entity',
                    toNode: '9'
                })
                .subscribe(response => {
                    expect(response).to.be.null;
                }, null, done);
        });
    });

    describe('deleteEdges', () => {
        it('should throw if invalid', () => {
            expect(() => store.deleteEdges()).to.throw('namespace is missing or wrong.');
        });

        it('should delete all edges', done => {
            store.deleteEdges({
                    namespace: app.namespace
                })
                .toArray()
                .mergeMap(response => {
                    expect(response.length).to.equal(6);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    return store._getEdgesKeys({
                        namespace: app.namespace
                    });
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(0);
                }, null, done);
        });

        it('should delete edges by fromNode', done => {
            store.deleteEdges({
                    fromNode: '1',
                    namespace: app.namespace
                })
                .toArray()
                .mergeMap(response => {
                    expect(response.length).to.equal(4);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    return store._getEdgesKeys({
                        namespace: app.namespace
                    });
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(2);
                    expect(response).to.deep.include('spec:0:entity');
                    expect(response).to.deep.include('spec:2:entity');
                }, null, done);
        });

        it('should delete edges by fromNode and entity', done => {
            store.deleteEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    namespace: app.namespace
                })
                .toArray()
                .mergeMap(response => {
                    expect(response.length).to.equal(2);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    return store._getEdgesKeys({
                        namespace: app.namespace
                    });
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(3);
                    expect(response).to.deep.include('spec:0:entity');
                    expect(response).to.deep.include('spec:1:entity');
                    expect(response).to.deep.include('spec:2:entity');
                }, null, done);
        });

        it('should delete edges by fromNode, entity and direction', done => {
            store.deleteEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT',
                    namespace: app.namespace
                })
                .toArray()
                .mergeMap(response => {
                    expect(response.length).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    return store._getEdgesKeys({
                        namespace: app.namespace
                    });
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(3);
                    expect(response).to.deep.include('spec:0:entity');
                    expect(response).to.deep.include('spec:1:entity');
                    expect(response).to.deep.include('spec:2:entity');
                }, null, done);
        });
    });

    describe('getEdges', () => {
        it('should throw if invalid', () => {
            expect(() => store.getEdges()).to.throw('namespace is missing or wrong.');
        });

        it('should get all edges', done => {
            store.getEdges({
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(6);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);
                }, null, done);
        });

        it('should get all edges by fromNode', done => {
            store.getEdges({
                    fromNode: '1',
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(4);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);
                }, null, done);
        });

        it('should get all edges by fromNode (without inverse)', done => {
            store.getEdges({
                    fromNode: '1',
                    namespace: app.namespace,
                    inverse: false
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should get all edges by fromNode and entity', done => {
            store.getEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);
                }, null, done);
        });

        it('should get all edges by fromNode, entity and direction', done => {
            store.getEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT',
                    namespace: app.namespace
                })
                .toArray()
                .subscribe(response => {
                    expect(response.length).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);
                }, null, done);
        });
    });

    describe('getEdgesByDistance', () => {
        beforeEach(done => {
            Observable.forkJoin(
                    store.setEdge({
                        distance: 0.8,
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: app.namespace,
                        toNode: '3'
                    }),
                    store.setEdge({
                        distance: 0.9,
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: app.namespace,
                        toNode: '4'
                    })
                )
                .subscribe(null, null, done);
        });

        it('should throw if invalid', () => {
            expect(() => store.getEdgesByDistance()).to.throw('entity, fromNode, namespace are missing or wrong.');
        });

        it('should throw if wrong distance', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity',
                    distance: 1,
                    fromNode: '1'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('distance should be an array like [min?: number, max?: number].');
                    done();
                });
        });

        it('should throw if wrong limit', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity',
                    limit: 1,
                    fromNode: '1'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('limit should be an array like [offset: number, count: number].');
                    done();
                });
        });

        it('should get edges', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    direction: 'OUT'
                })
                .toArray()
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '3'
                    }, {
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '4'
                    }, {
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should get edges desc', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    direction: 'OUT',
                    desc: true
                })
                .toArray()
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }, {
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '4'
                    }, {
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '3'
                    }]);
                }, null, done);
        });

        it('should get by min distance', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    distance: [1],
                    direction: 'OUT'
                })
                .toArray()
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should get by max distance', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    distance: [, 0.8],
                    direction: 'OUT'
                })
                .toArray()
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '3'
                    }]);
                }, null, done);
        });

        it('should get by distance range', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    distance: [0.8, 0.9],
                    direction: 'OUT'
                })
                .toArray()
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '3'
                    }, {
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '4'
                    }]);
                }, null, done);
        });

        it('should get by limit', done => {
            store.getEdgesByDistance({
                    namespace: app.namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    limit: [0, 1],
                    direction: 'OUT'
                })
                .toArray()
                .mergeMap(response => {
                    expect(response[0].toNode).to.equal('3');

                    return store.getEdgesByDistance({
                        namespace: app.namespace,
                        entity: 'entity-2',
                        fromNode: '1',
                        limit: [1, 1],
                        direction: 'OUT'
                    });
                })
                .toArray()
                .subscribe(response => {
                    expect(response[0].toNode).to.equal('4');
                }, null, done);
        });
    });

    describe('setEdge', () => {
        it('should throw if invalid', () => {
            expect(() => store.setEdge()).to.throw('distance, entity, fromNode, namespace, toNode are missing or wrong.');
        });

        it('should return inserted edges', done => {
            store.setEdge({
                    namespace: app.namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: 1
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: null,
                        distance: 1,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 1,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);
                }, null, done);
        });

        it('should return inserted edges with direction', done => {
            store.setEdge({
                    namespace: app.namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: 1,
                    direction: 'OUT'
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }, {
                        direction: 'IN',
                        distance: 1,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);
                }, null, done);
        });
    });

    describe('incrementEdge', () => {
        it('should throw if invalid', () => {
            expect(() => store.incrementEdge()).to.throw('distance, entity, fromNode, namespace, toNode are missing or wrong.');
        });

        it('should return inserted edges', done => {
            store.incrementEdge({
                    namespace: app.namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: -0.1
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: null,
                        distance: 0.9,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 0.9,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);
                }, null, done);
        });

        it('should return inserted edges without increment', done => {
            store.incrementEdge({
                    namespace: app.namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: 0
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: null,
                        distance: 1,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 1,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);
                }, null, done);
        });

        it('should return inserted edges with direction', done => {
            store.incrementEdge({
                    namespace: app.namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: -0.1,
                    direction: 'OUT'
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: 'spec',
                        toNode: '1'
                    }, {
                        direction: 'IN',
                        distance: 0.9,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '0'
                    }]);
                }, null, done);
        });
    });
});