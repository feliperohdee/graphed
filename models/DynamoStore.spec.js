const _ = require('lodash');
const chai = require('chai');
const chaiSubset = require('chai-subset');
const rx = require('rxjs');
const rxop = require('rxjs/operators');

const testing = require('../testing');
const {
    DynamoStore
} = require('../');

chai.use(chaiSubset);

const expect = chai.expect;
const namespace = 'spec';

const {
    store
} = testing.app;

describe('models/DynamoStore.js', () => {
    beforeEach(done => {
        rx.forkJoin(
                store.setEdge({
                    distance: 1,
                    entity: 'entity',
                    fromNode: '0',
                    namespace,
                    toNode: '1'
                }),
                store.setEdge({
                    distance: 1,
                    entity: 'entity',
                    fromNode: '0',
                    namespace,
                    toNode: '2'
                }),
                store.setEdge({
                    distance: 1,
                    direction: 'OUT',
                    entity: 'entity-2',
                    fromNode: '1',
                    namespace,
                    toNode: '2'
                })
            )
            .subscribe(testing.rx(null, null, done));
    });

    afterEach(done => {
        rx.forkJoin(
                store.deleteEdges({
                    fromNode: '0',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                ),
                store.deleteEdges({
                    fromNode: '1',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                ),
                store.deleteEdges({
                    fromNode: '2',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
            )
            .subscribe(testing.rx(null, null, done));
    });

    after(done => {
        testing.app.store.clear({
                namespace
            })
            .subscribe(testing.rx(null, null, done));
    });

    describe('constructor', () => {
        it('should throw if no dynamodb provided', () => {
            expect(() => new DynamoStore({})).to.throw('"dynamodb" is required');
        });

        it('should throw if no tableName provided', () => {
            expect(() => new DynamoStore({
                dynamodb: testing.app.dynamodb
            })).to.throw('"tableName" is required');
        });
    });

    describe('_composeBase', () => {
        it('should compose index with keys', () => {
            expect(store._composeBase({
                namespace,
                fromNode: 'fromNode'
            })).to.equal('spec:fromNode');

            expect(store._composeBase({
                namespace,
                fromNode: 'fromNode'
            }, true)).to.equal('spec:fromNode:~');

            expect(store._composeBase({
                namespace,
                entity: 'entity',
                fromNode: 'fromNode'
            })).to.equal('spec:fromNode:entity');

            expect(store._composeBase({
                namespace,
                entity: 'entity',
                fromNode: 'fromNode'
            }, true)).to.equal('spec:fromNode:entity:~');

            expect(store._composeBase({
                namespace,
                direction: 'direction',
                entity: 'entity',
                fromNode: 'fromNode'
            })).to.equal('spec:fromNode:entity:direction');
        });
    });

    describe('_composeId', () => {
        it('should compose id with keys', () => {
            expect(store._composeId({
                fromNode: 'fromNode'
            })).to.equal('fromNode');

            expect(store._composeId({
                fromNode: 'fromNode'
            }, true)).to.equal('fromNode:~');

            expect(store._composeId({
                entity: 'entity',
                fromNode: 'fromNode'
            })).to.equal('fromNode:entity');

            expect(store._composeId({
                entity: 'entity',
                fromNode: 'fromNode'
            }, true)).to.equal('fromNode:entity:~');

            expect(store._composeId({
                direction: 'direction',
                entity: 'entity',
                fromNode: 'fromNode'
            })).to.equal('fromNode:entity:direction');

            expect(store._composeId({
                direction: 'direction',
                entity: 'entity',
                fromNode: 'fromNode',
                toNode: 'toNode'
            })).to.equal('fromNode:entity:direction:toNode');
        });
    });

    describe('_parseId', () => {
        it('should parse id', () => {
            expect(store._parseId('fromNode:entity:OUT:toNode')).to.deep.equal({
                direction: 'OUT',
                entity: 'entity',
                fromNode: 'fromNode',
                toNode: 'toNode'
            });

            expect(store._parseId('fromNode:entity:IN:toNode')).to.deep.equal({
                direction: 'IN',
                entity: 'entity',
                fromNode: 'fromNode',
                toNode: 'toNode'
            });

            expect(store._parseId('fromNode:entity:~:toNode')).to.deep.equal({
                direction: null,
                entity: 'entity',
                fromNode: 'fromNode',
                toNode: 'toNode'
            });
        });
    });

    describe('countEdges', () => {
        it('should throw if invalid', done => {
            store.countEdges()
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"entity" is required. "fromNode" is required. "namespace" is required');
                }, null, done));
        });

        it('should return count', done => {
            store.countEdges({
                    namespace,
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT'
                })
                .subscribe(testing.rx(response => {
                    expect(response).to.equal(1);
                }, null, done));
        });
    });

    describe('deleteEdge', () => {
        it('should throw if invalid', done => {
            store.deleteEdge()
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"entity" is required. "fromNode" is required. "namespace" is required. "toNode" is required');
                }, null, done));
        });

        it('should return deleted edge', done => {
            store.deleteEdge({
                    namespace,
                    fromNode: '0',
                    entity: 'entity',
                    toNode: '1'
                })
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal({
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    });
                }, null, done));
        });

        it('should return deleted edges', done => {
            store.deleteEdge({
                    namespace,
                    fromNode: '0',
                    entity: 'entity',
                    inverse: true,
                    toNode: '1'
                })
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }, {
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);
                }, null, done));
        });

        it('should return null if no edges delete', done => {
            store.deleteEdge({
                    namespace,
                    fromNode: '0',
                    entity: 'entity',
                    toNode: '9'
                })
                .subscribe(testing.rx(response => {
                    expect(response).to.be.null;
                }, null, done));
        });
    });

    describe('deleteEdges', () => {
        it('should throw if invalid', done => {
            store.deleteEdges()
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"namespace" is required');
                }, null, done));
        });

        it('should delete all edges', done => {
            store.deleteEdges({
                    namespace
                })
                .pipe(
                    rxop.toArray(),
                    rxop.mergeMap(response => {
                        expect(_.size(response)).to.equal(6);
                        expect(response).to.containSubset([{
                            direction: null,
                            entity: 'entity',
                            fromNode: '0',
                            namespace,
                            toNode: '1'
                        }]);

                        expect(response).to.containSubset([{
                            direction: null,
                            entity: 'entity',
                            fromNode: '1',
                            namespace,
                            toNode: '0'
                        }]);

                        expect(response).to.containSubset([{
                            direction: null,
                            entity: 'entity',
                            fromNode: '0',
                            namespace,
                            toNode: '2'
                        }]);

                        expect(response).to.containSubset([{
                            direction: null,
                            entity: 'entity',
                            fromNode: '2',
                            namespace,
                            toNode: '0'
                        }]);

                        expect(response).to.containSubset([{
                            direction: 'OUT',
                            entity: 'entity-2',
                            fromNode: '1',
                            namespace,
                            toNode: '2'
                        }]);

                        expect(response).to.containSubset([{
                            direction: 'IN',
                            entity: 'entity-2',
                            fromNode: '2',
                            namespace,
                            toNode: '1'
                        }]);

                        return store.fetch({
                            namespace
                        });
                    })
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response.items)).to.equal(0);
                }, null, done));
        });

        it('should delete edges by fromNode', done => {
            store.deleteEdges({
                    fromNode: '1',
                    namespace
                })
                .pipe(
                    rxop.toArray(),
                    rxop.mergeMap(response => {
                        expect(_.size(response)).to.equal(4);

                        expect(response).to.containSubset([{
                            direction: null,
                            entity: 'entity',
                            fromNode: '1',
                            namespace,
                            toNode: '0'
                        }]);

                        expect(response).to.containSubset([{
                            direction: null,
                            entity: 'entity',
                            fromNode: '0',
                            namespace,
                            toNode: '1'
                        }]);

                        expect(response).to.containSubset([{
                            direction: 'OUT',
                            entity: 'entity-2',
                            fromNode: '1',
                            namespace,
                            toNode: '2'
                        }]);

                        expect(response).to.containSubset([{
                            direction: 'IN',
                            entity: 'entity-2',
                            fromNode: '2',
                            namespace,
                            toNode: '1'
                        }]);

                        return store.fetch({
                            namespace
                        });
                    })
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response.items)).to.equal(2);
                    expect(response.items[0].base).to.deep.include('spec:0:entity');
                    expect(response.items[1].base).to.deep.include('spec:2:entity');
                }, null, done));
        });

        it('should delete edges by fromNode and entity', done => {
            store.deleteEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    namespace
                })
                .pipe(
                    rxop.toArray(),
                    rxop.mergeMap(response => {
                        expect(_.size(response)).to.equal(2);
                        expect(response).to.containSubset([{
                            direction: 'OUT',
                            entity: 'entity-2',
                            fromNode: '1',
                            namespace,
                            toNode: '2'
                        }]);

                        expect(response).to.containSubset([{
                            direction: 'IN',
                            entity: 'entity-2',
                            fromNode: '2',
                            namespace,
                            toNode: '1'
                        }]);

                        return store.fetch({
                            namespace
                        });
                    })
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response.items)).to.equal(4);
                    expect(response.items[0].base).to.deep.include('spec:0:entity');
                    expect(response.items[2].base).to.deep.include('spec:1:entity');
                    expect(response.items[3].base).to.deep.include('spec:2:entity');
                }, null, done));
        });

        it('should delete edges by fromNode, entity and direction', done => {
            store.deleteEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT',
                    namespace
                })
                .pipe(
                    rxop.toArray(),
                    rxop.mergeMap(response => {
                        expect(_.size(response)).to.equal(2);
                        expect(response).to.containSubset([{
                            direction: 'OUT',
                            entity: 'entity-2',
                            fromNode: '1',
                            namespace,
                            toNode: '2'
                        }]);

                        expect(response).to.containSubset([{
                            direction: 'IN',
                            entity: 'entity-2',
                            fromNode: '2',
                            namespace,
                            toNode: '1'
                        }]);

                        return store.fetch({
                            namespace
                        });
                    })
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response.items)).to.equal(4);
                    expect(response.items[0].base).to.deep.include('spec:0:entity');
                    expect(response.items[2].base).to.deep.include('spec:1:entity');
                    expect(response.items[3].base).to.deep.include('spec:2:entity');
                }, null, done));
        });
    });

    describe('getEdges', () => {
        it('should throw if invalid', done => {
            store.getEdges()
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"namespace" is required');
                }, null, done));
        });

        it('should get all edges', done => {
            store.getEdges({
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response)).to.equal(6);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);
                }, null, done));
        });

        it('should get all edges by fromNode', done => {
            store.getEdges({
                    fromNode: '1',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response)).to.equal(4);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);
                }, null, done));
        });

        it('should get all edges by fromNode (without inverse)', done => {
            store.getEdges({
                    fromNode: '1',
                    namespace,
                    inverse: false
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response)).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done));
        });

        it('should get all edges by fromNode and entity', done => {
            store.getEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response)).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);
                }, null, done));
        });

        it('should get all edges by fromNode, entity and direction', done => {
            store.getEdges({
                    fromNode: '1',
                    entity: 'entity-2',
                    direction: 'OUT',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response)).to.equal(2);
                    expect(response).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(response).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity-2',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);
                }, null, done));
        });
    });

    describe('getEdgesByDistance', () => {
        beforeEach(done => {
            rx.forkJoin(
                    store.setEdge({
                        distance: 0.8,
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    }),
                    store.setEdge({
                        distance: 0.9,
                        direction: 'OUT',
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '4'
                    })
                )
                .subscribe(testing.rx(null, null, done));
        });

        it('should throw if invalid', done => {
            store.getEdgesByDistance()
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"entity" is required. "fromNode" is required. "namespace" is required');
                }, null, done));
        });

        it('should throw if wrong distance', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity',
                    distance: 1,
                    fromNode: '1'
                })
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"distance" must be an array');
                }, null, done));
        });

        it('should get edges', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    direction: 'OUT'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    }, {
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '4'
                    }, {
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done));
        });

        it('should get edges desc', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    direction: 'OUT',
                    desc: true
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }, {
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '4'
                    }, {
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    }]);
                }, null, done));
        });

        it('should get by min distance', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    distance: [1],
                    direction: 'OUT'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done));
        });

        it('should get by max distance', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    distance: [0, 0.8],
                    direction: 'OUT'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    }]);
                }, null, done));
        });

        it('should get by distance range', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    distance: [0.8, 0.9],
                    direction: 'OUT'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.8,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    }, {
                        direction: 'OUT',
                        distance: 0.9,
                        entity: 'entity-2',
                        fromNode: '1',
                        namespace,
                        toNode: '4'
                    }]);
                }, null, done));
        });

        it('should get by limit', done => {
            store.getEdgesByDistance({
                    namespace,
                    entity: 'entity-2',
                    fromNode: '1',
                    limit: 1,
                    direction: 'OUT'
                })
                .pipe(
                    rxop.toArray(),
                    rxop.mergeMap(response => {
                        expect(_.size(response)).to.equal(1);

                        return store.getEdgesByDistance({
                            namespace,
                            entity: 'entity-2',
                            fromNode: '1',
                            limit: 2,
                            direction: 'OUT'
                        });
                    }),
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(_.size(response)).to.equal(2);
                }, null, done));
        });
    });

    describe('setEdge', () => {
        it('should throw if invalid', done => {
            store.setEdge()
                .subscribe(null, testing.rx(err => {
                    expect(err.message).to.equal('"distance" is required. "entity" is required. "fromNode" is required. "namespace" is required. "toNode" is required');
                }, null, done));
        });

        it('should return inserted edges', done => {
            store.setEdge({
                    namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: 1
                })
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: null,
                        distance: 1,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 1,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);
                }, null, done));
        });

        it('should return inserted edges with direction', done => {
            store.setEdge({
                    namespace,
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1',
                    distance: 1,
                    direction: 'OUT'
                })
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }, {
                        direction: 'IN',
                        distance: 1,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);
                }, null, done));
        });

        describe('increment', () => {
            it('should return inserted edges', done => {
                store.setEdge({
                        namespace,
                        entity: 'entity',
                        fromNode: '0',
                        toNode: '1',
                        distance: -.1
                    }, true)
                    .subscribe(testing.rx(response => {
                        expect(response).to.deep.equal([{
                            direction: null,
                            distance: 0.9,
                            entity: 'entity',
                            fromNode: '0',
                            namespace,
                            toNode: '1'
                        }, {
                            direction: null,
                            distance: 0.9,
                            entity: 'entity',
                            fromNode: '1',
                            namespace,
                            toNode: '0'
                        }]);
                    }, null, done));
            });

            it('should return inserted edges without increment', done => {
                store.setEdge({
                        namespace,
                        entity: 'entity',
                        fromNode: '0',
                        toNode: '1',
                        distance: 0
                    }, true)
                    .subscribe(testing.rx(response => {
                        expect(response).to.deep.equal([{
                            direction: null,
                            distance: 1,
                            entity: 'entity',
                            fromNode: '0',
                            namespace,
                            toNode: '1'
                        }, {
                            direction: null,
                            distance: 1,
                            entity: 'entity',
                            fromNode: '1',
                            namespace,
                            toNode: '0'
                        }]);
                    }, null, done));
            });

            it('should return inserted edges with direction', done => {
                store.setEdge({
                        namespace,
                        entity: 'entity',
                        fromNode: '0',
                        toNode: '1',
                        distance: -.1,
                        direction: 'OUT'
                    }, true)
                    .subscribe(testing.rx(response => {
                        expect(response).to.deep.equal([{
                            direction: 'OUT',
                            distance: .9,
                            entity: 'entity',
                            fromNode: '0',
                            namespace,
                            toNode: '1'
                        }, {
                            direction: 'IN',
                            distance: .9,
                            entity: 'entity',
                            fromNode: '1',
                            namespace,
                            toNode: '0'
                        }]);
                    }, null, done));
            });
        });
    });
});