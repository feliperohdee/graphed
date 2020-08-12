const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const chaiSubset = require('chai-subset');
const rx = require('rxjs');
const rxop = require('rxjs/operators');

const app = require('../testing/dynamoDb');
const {
    AWS
} = require('../libs');
const {
    Graph
} = require('../');

chai.use(chaiSubset);
chai.use(sinonChai);

const expect = chai.expect;
const namespace = 'spec';

describe('models/Graph.js', () => {
    let graph;

    before(() => {
        graph = new Graph({
            partition: app.partition,
            store: app.store
        });
    });

    beforeEach(() => {
        graph = new Graph({
            partition: app.partition,
            store: app.store
        });
    });

    describe('constructor', () => {
        it('should have store based functions', () => {
            expect(graph.countEdges).not.to.be.undefined;
            expect(graph.deleteEdge).not.to.be.undefined;
            expect(graph.deleteEdges).not.to.be.undefined;
            expect(graph.getAll).not.to.be.undefined;
            expect(graph.getAllByDistance).not.to.be.undefined;
            expect(graph.setEdge).not.to.be.undefined;
        });
    });

    describe('allByNode', () => {
        before(done => {
            rx.forkJoin(
                    graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    graph.link({
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should fetch all by node', done => {
            rx.forkJoin(
                    graph.allByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.allByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '3',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should fetch all by node only by entity and direction', done => {
            rx.forkJoin(
                    graph.allByNode({
                        fromNode: '1',
                        direction: 'OUT',
                        entity: 'entity'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.allByNode({
                        fromNode: '2',
                        direction: 'OUT',
                        entity: 'entity'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.deep.equal([]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '3',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should fetch all by node only by distance', done => {
            rx.forkJoin(
                    graph.allByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.allByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '3',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should fetch all by node without inverse', done => {
            rx.forkJoin(
                    graph.allByNode({
                        fromNode: '1',
                        noInverse: true
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.allByNode({
                        fromNode: '2',
                        noInverse: true
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    }]);
                }, null, done);
        });

        it('should fetch all by node returning only nodes', done => {
            rx.forkJoin(
                    graph.allByNode({
                        fromNode: '1',
                        onlyNodes: true
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.allByNode({
                        fromNode: '2',
                        noInverse: true,
                        onlyNodes: true
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.deep.contain('2');
                    expect(operation2).to.deep.contain('3');
                    expect(operation2).to.deep.contain('1');
                }, null, done);
        });
    });

    describe('closest', () => {
        before(done => {
            rx.forkJoin(
                    graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    graph.link({
                        absoluteDistance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '3'
                    }),
                    graph.link({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '3'
                    }),
                    graph.link({
                        absoluteDistance: 0.999999999999998,
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '4'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '3'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '4'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should get closest nodes', done => {
            graph.closest({
                    entity: 'entity',
                    fromNode: '1'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response[0]).to.deep.equal({
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    });

                    expect(response[1]).to.deep.equal({
                        direction: null,
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    });
                }, null, done);
        });

        it('should get closest nodes with direction', done => {
            graph.closest({
                    direction: 'OUT',
                    entity: 'entity',
                    fromNode: '2'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response[0]).to.deep.equal({
                        direction: 'OUT',
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '4'
                    });

                    expect(response[1]).to.deep.equal({
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    });
                }, null, done);
        });

        it('should get closest nodes desc', done => {
            graph.closest({
                    desc: true,
                    entity: 'entity',
                    fromNode: '1'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response[1]).to.deep.equal({
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    });

                    expect(response[0]).to.deep.equal({
                        direction: null,
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    });
                }, null, done);
        });

        it('should get closest nodes with limit', done => {
            graph.closest({
                    limit: 1,
                    entity: 'entity',
                    fromNode: '1'
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response[0]).to.deep.equal({
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '3'
                    });
                }, null, done);
        });
    });

    describe('count', () => {
        before(done => {
            rx.forkJoin(
                    graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '3'
                    }),
                    graph.link({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '3'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should return elements count', done => {
            rx.forkJoin(
                    graph.count({
                        entity: 'entity',
                        fromNode: '1'
                    }),
                    graph.count({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '1'
                    }),
                    graph.count({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '1'
                    }),
                    graph.count({
                        entity: 'entity',
                        fromNode: '2'
                    }),
                    graph.count({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2'
                    }),
                    graph.count({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '2'
                    }),
                    graph.count({
                        entity: 'entity',
                        fromNode: '3'
                    }),
                    graph.count({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '3'
                    }),
                    graph.count({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '3'
                    })
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([
                        2, 0, 0,
                        1, 1, 0,
                        1, 0, 1
                    ]);
                }, null, done);
        });
    });

    describe('crossLink', () => {
        afterEach(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '0'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '3'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should not add multiple single edge', done => {
            graph.crossLink({
                    entity: 'entity',
                    value: [
                        '0'
                    ]
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(_.size(response)).to.equal(0);
                }, null, done);
        });

        it('should add multiple edges', done => {
            graph.crossLink({
                    entity: 'entity',
                    value: [
                        '0',
                        '1',
                        '2',
                        '3'
                    ]
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    response = _.flattenDeep(response);
                    expect(_.size(response)).to.equal(12);

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;
                }, null, done);
        });

        it('should add multiple edges with direction', done => {
            graph.crossLink({
                    direction: 'OUT',
                    entity: 'entity',
                    value: [
                        '0',
                        '1',
                        '2',
                        '3'
                    ]
                })
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    response = _.flattenDeep(response);
                    expect(_.size(response)).to.equal(24);

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    // 
                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    // 
                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    // 
                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '0'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '0',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '1'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '1',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '3',
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        toNode: '2'
                    })).to.be.true;

                    expect(!!_.find(response, {
                        entity: 'entity',
                        fromNode: '2',
                        direction: 'IN',
                        distance: 0.999999999999999,
                        toNode: '3'
                    })).to.be.true;
                }, null, done);
        });

        describe('origin', () => {
            it('should add multiple single edge', done => {
                graph.crossLink({
                        distance: 1,
                        entity: 'entity',
                        origin: '0',
                        value: [
                            '1'
                        ]
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        response = _.flattenDeep(response);
                        expect(_.size(response)).to.equal(2);
                        
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '1',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
                    }, null, done);
            });

            it('should add multiple edges', done => {
                graph.crossLink({
                        distance: 1,
                        entity: 'entity',
                        origin: '0',
                        value: [
                            '1',
                            '2',
                            '3'
                        ]
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        response = _.flattenDeep(response);
                        expect(_.size(response)).to.equal(12);
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '1',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '2',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '3',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
                        
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '1',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '2',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '2',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '3',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '1',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '3',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
                    }, null, done);
            });
            
            it('should add multiple edges no crossing', done => {
                graph.crossLink({
                        cross: false,
                        distance: 1,
                        entity: 'entity',
                        origin: '0',
                        value: [
                            '1',
                            '2',
                            '3'
                        ]
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        response = _.flattenDeep(response);
                        expect(_.size(response)).to.equal(6);
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '1',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '2',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '3',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
                    }, null, done);
            });
            
            it('should add multiple edges with direction', done => {
                graph.crossLink({
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity',
                        origin: '0',
                        value: [
                            '1',
                            '2',
                            '3'
                        ]
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        response = _.flattenDeep(response);
                        expect(_.size(response)).to.equal(18);
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '1',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '2',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '3',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        //     
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '1',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '2',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '1',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '3',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
    
                        //     
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '2',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '1',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '2',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '3',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;
    
                        //     
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '3',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '1',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '3',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'x-entity',
                            fromNode: '2',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;
                    }, null, done);
            });
            
            it('should add multiple edges with direction no crossing', done => {
                graph.crossLink({
                        cross: false,
                        direction: 'OUT',
                        distance: 1,
                        entity: 'entity',
                        origin: '0',
                        value: [
                            '1',
                            '2',
                            '3'
                        ]
                    })
                    .pipe(
                        rxop.toArray()
                    )
                    .subscribe(response => {
                        response = _.flattenDeep(response);
                        expect(_.size(response)).to.equal(6);
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '1'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '1',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '2'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '2',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
    
                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '0',
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            toNode: '3'
                        })).to.be.true;

                        expect(!!_.find(response, {
                            entity: 'entity',
                            fromNode: '3',
                            direction: 'IN',
                            distance: 0.999999999999999,
                            toNode: '0'
                        })).to.be.true;
                    }, null, done);
            });
        });
    });

    describe('delete', () => {
        beforeEach(done => {
            rx.forkJoin(
                    graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    graph.link({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        describe('with direction', () => {
            it('should delete a edge', done => {
                graph.delete({
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        toNode: '3'
                    })
                    .subscribe(response => {
                        expect(response.fromNode).to.deep.equal('2');
                    }, null, done);
            });

            it('should delete two edges', done => {
                graph.delete({
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        inverse: true,
                        toNode: '3'
                    })
                    .subscribe(([
                        edge1,
                        edge2
                    ]) => {
                        expect(edge1.fromNode).to.deep.equal('2');
                        expect(edge1.toNode).to.deep.equal('3');
                        expect(edge2.fromNode).to.deep.equal('3');
                        expect(edge2.toNode).to.deep.equal('2');
                    }, null, done);
            });
        });

        describe('without direction', () => {
            it('should delete a edge', done => {
                graph.delete({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    })
                    .subscribe(response => {
                        expect(response.fromNode).to.deep.equal('1');
                    }, null, done);
            });

            it('should delete two edges', done => {
                graph.delete({
                        entity: 'entity',
                        fromNode: '1',
                        inverse: true,
                        toNode: '2'
                    })
                    .subscribe(([
                        edge1,
                        edge2
                    ]) => {
                        expect(edge1.fromNode).to.deep.equal('1');
                        expect(edge1.toNode).to.deep.equal('2');
                        expect(edge2.fromNode).to.deep.equal('2');
                        expect(edge2.toNode).to.deep.equal('1');
                    }, null, done);
            });
        });
    });

    describe('deleteByNode', () => {
        beforeEach(done => {
            rx.forkJoin(
                    graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    graph.link({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should delete by fromNode', done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '3'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    fromNode1,
                    fromNode3
                ]) => {
                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(fromNode3).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    }]);

                    expect(fromNode3).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '3',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should delete by fromNode and entity', done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1',
                        entity: 'entity'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2',
                        entity: 'inexistent'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    fromNode1,
                    fromNode2
                ]) => {
                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(fromNode2).to.deep.equal([]);
                }, null, done);
        });

        it('should delete by fromNode, entity and direction', done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1',
                        entity: 'entity',
                        direction: 'IN'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2',
                        entity: 'entity',
                        direction: 'IN'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(([
                    fromNode1,
                    fromNode2
                ]) => {
                    expect(fromNode1).to.deep.equal([]);

                    expect(fromNode2).not.to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '1'
                    }]);

                    expect(fromNode2).not.to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '2'
                    }]);

                    expect(fromNode2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '2',
                        namespace,
                        toNode: '3'
                    }]);

                    expect(fromNode2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '3',
                        namespace,
                        toNode: '2'
                    }]);
                }, null, done);
        });
    });

    describe('link', () => {
        afterEach(done => {
            rx.forkJoin(
                    graph.deleteByNode({
                        fromNode: '1'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    graph.deleteByNode({
                        fromNode: '2'
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should insert two edges with absoluteDistance', done => {
            graph.link({
                    absoluteDistance: 0.999999999999999,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(0.999999999999999);
                    expect(secondEdge.distance).to.equal(0.999999999999999);
                }, null, done);
        });

        it('should decrement edges distance', done => {
            graph.link({
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(0.999999999999999);
                    expect(secondEdge.distance).to.equal(0.999999999999999);
                }, null, done);
        });

        it('should decrement edges distance with custom distance', done => {
            graph.link({
                    distance: 5,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(0.999999999999995);
                    expect(secondEdge.distance).to.equal(0.999999999999995);
                }, null, done);
        });

        it('should not decrement edges distance (without previous edge)', done => {
            graph.link({
                    distance: 0,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(1);
                    expect(secondEdge.distance).to.equal(1);
                }, null, done);
        });

        it('should not decrement edges distance', done => {
            graph.link({
                    distance: 5,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .pipe(
                    rxop.mergeMap(() => graph.link({
                        distance: 0,
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }))
                )
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(0.999999999999995);
                    expect(secondEdge.distance).to.equal(0.999999999999995);
                }, null, done);
        });

        it('should decrement edges distance with custom decrementPath', done => {
            graph.decrementPath = 0.1;
            graph.link({
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(0.9);
                    expect(secondEdge.distance).to.equal(0.9);
                }, null, done);
        });

        it('should increment edges distance', done => {
            graph.link({
                    distance: 1,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(0.999999999999999);
                    expect(secondEdge.distance).to.equal(0.999999999999999);
                }, null, done);
        });

        describe('without direction', () => {
            it('should insert two edges', done => {
                graph.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    })
                    .subscribe(response => {
                        const [
                            firstEdge,
                            secondEdge
                        ] = response;

                        expect(firstEdge.distance).to.equal(0.999999999999999);
                        expect(firstEdge.entity).to.equal('entity');
                        expect(firstEdge.fromNode).to.equal('1');
                        expect(firstEdge.toNode).to.equal('2');

                        expect(secondEdge.distance).to.equal(0.999999999999999);
                        expect(secondEdge.entity).to.equal('entity');
                        expect(secondEdge.fromNode).to.equal('2');
                        expect(secondEdge.toNode).to.equal('1');
                    }, null, done);
            });
        });

        describe('with direction', () => {
            it('should insert two edges', done => {
                graph.link({
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    })
                    .subscribe(response => {
                        const [
                            firstEdge,
                            secondEdge
                        ] = response;

                        expect(firstEdge.direction).to.equal('OUT');
                        expect(firstEdge.distance).to.equal(0.999999999999999);
                        expect(firstEdge.entity).to.equal('entity');
                        expect(firstEdge.fromNode).to.equal('1');
                        expect(firstEdge.toNode).to.equal('2');

                        expect(secondEdge.direction).to.equal('IN');
                        expect(secondEdge.distance).to.equal(0.999999999999999);
                        expect(secondEdge.entity).to.equal('entity');
                        expect(secondEdge.fromNode).to.equal('2');
                        expect(secondEdge.toNode).to.equal('1');
                    }, null, done);
            });
        });

        describe('with firehose', () => {
            const edgeFirehose = new Graph({
                partition: app.partition,
                store: app.store
            }, {
                firehose: {
                    concurrency: 1,
                    stream: 'stream'
                }
            });

            beforeEach(() => {
                sinon.stub(AWS.firehose, 'putRecord')
                    .returns({
                        promise: () => Promise.resolve({
                            RecordId: 'RecordId',
                            Encrypted: false
                        })
                    });
            });

            afterEach(() => {
                AWS.firehose.putRecord.restore();
            });

            it('should call putRecord', done => {
                edgeFirehose.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    })
                    .subscribe(response => {
                        expect(AWS.firehose.putRecord).to.have.been.calledOnceWithExactly({
                            DeliveryStreamName: 'stream',
                            Record: {
                                Data: JSON.stringify({
                                    entity: 'entity',
                                    fromNode: '1',
                                    toNode: '2',
                                    direction: null,
                                    distance: 1
                                }) + '\n'
                            }
                        });
                    }, null, done);
            });

            it('should not call putRecord if fromFirehose = true', done => {
                edgeFirehose.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }, true)
                    .subscribe(response => {
                        expect(AWS.firehose.putRecord).to.not.have.been.called;
                    }, null, done);
            });
        });
    });

    describe('processFirehose', () => {
        const edgeFirehose = new Graph({
            partition: app.partition,
            store: app.store
        }, {
            firehose: {
                concurrency: 1,
                stream: 'stream'
            }
        });

        beforeEach(() => {
            sinon.spy(edgeFirehose, 'link');
        });

        afterEach(done => {
            edgeFirehose.link.restore();

            rx.forkJoin(
                    edgeFirehose.deleteByNode({
                        fromNode: '0'
                    })
                    .pipe(
                        rxop.toArray()
                    ),
                    edgeFirehose.deleteByNode({
                        fromNode: '0',
                        namespace
                    })
                    .pipe(
                        rxop.toArray()
                    )
                )
                .subscribe(null, null, done);
        });

        it('should throw if no firehose configured', done => {
            graph.processFirehose()
                .subscribe(null, err => {
                    expect(err.message).to.equal('no firehose configured.');
                    done();
                });
        });

        it('should process', done => {
            const stream = rx.from([{
                distance: 2,
                entity: 'entity',
                fromNode: '0',
                toNode: '1'
            }, {
                entity: 'entity',
                fromNode: '0',
                toNode: '1'
            }, {
                absoluteDistance: 1,
                entity: 'entity',
                fromNode: '0',
                toNode: '1'
            }, {
                entity: 'entity',
                fromNode: '0',
                toNode: '1'
            }, {
                entity: 'entity',
                fromNode: '0',
                toNode: '1'
            }, {
                entity: 'entity',
                fromNode: '0',
                toNode: '1',
                namespace
            }, {
                direction: 'OUT',
                entity: 'entity',
                fromNode: '0',
                toNode: '1',
                namespace
            }, {
                direction: 'OUT',
                entity: 'entity',
                fromNode: '1',
                toNode: '0',
                namespace
            }]);

            edgeFirehose.processFirehose(stream)
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(edgeFirehose.link).to.have.callCount(6);
                    expect(edgeFirehose.link).to.have.been.calledWith(sinon.match.object, true);

                    // 0 <-> (2 + 1) <-> 1
                    expect(response[0]).to.deep.equal([{
                        direction: null,
                        distance: 0.999999999999997,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 0.999999999999997,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);

                    // 0 <-> abs(1) <-> 1
                    expect(response[1]).to.deep.equal([{
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

                    // 0 <-> (1 + 1) <-> 1
                    expect(response[2]).to.deep.equal([{
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);

                    // namespace 0 <-> (1) <-> 1
                    expect(response[3]).to.deep.equal([{
                        direction: null,
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: `${namespace}:${namespace}`,
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: `${namespace}:${namespace}`,
                        toNode: '0'
                    }]);

                    // namespace 0 -> (1) -> 1
                    expect(response[4]).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: `${namespace}:${namespace}`,
                        toNode: '1'
                    }, {
                        direction: 'IN',
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: `${namespace}:${namespace}`,
                        toNode: '0'
                    }]);

                    // namespace 1 -> (1) -> 0
                    expect(response[5]).to.deep.equal([{
                        direction: 'OUT',
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: `${namespace}:${namespace}`,
                        toNode: '0'
                    }, {
                        direction: 'IN',
                        distance: 0.999999999999999,
                        entity: 'entity',
                        fromNode: '0',
                        namespace: `${namespace}:${namespace}`,
                        toNode: '1'
                    }]);
                }, null, done);
        });

        it('should process and merge', done => {
            const link = {
                entity: 'entity',
                fromNode: '0',
                toNode: '1'
            };

            edgeFirehose.link(link, true)
                .pipe(
                    rxop.mergeMap(() => {
                        return edgeFirehose.processFirehose(rx.of(link));
                    }),
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(edgeFirehose.link).to.have.callCount(2);

                    // 0 <-> (2 + 1) <-> 1
                    expect(response[0]).to.deep.equal([{
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '0',
                        namespace,
                        toNode: '1'
                    }, {
                        direction: null,
                        distance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        namespace,
                        toNode: '0'
                    }]);
                }, null, done);
        });

        it('should skip invalid streams', done => {
            edgeFirehose.processFirehose(rx.of({
                    fromNode: '0',
                    toNode: '1'
                }))
                .pipe(
                    rxop.toArray()
                )
                .subscribe(response => {
                    expect(response).to.deep.equal([]);
                }, null, done);
        });
    });

    describe('traverse', () => {
        it('should return empty if no jobs', done => {
            graph.traverse({
                    jobs: []
                })
                .subscribe(response => {
                    expect(response).to.deep.equal([]);
                }, null, done);
        });

        it('should handle errors', done => {
            graph.traverse({
                    jobs: [{
                        entity: 'entity'
                    }]
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('"fromNode" is required');
                    done();
                });
        });

        describe('without direction', () => {
            before(done => {
                rx.forkJoin(
                        graph.link({
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '1'
                        }),
                        graph.link({
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '2'
                        }),
                        graph.link({
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '4'
                        }),
                        graph.link({
                            absoluteDistance: 0.999999999999998,
                            entity: 'entity',
                            fromNode: '2',
                            toNode: '3'
                        }),
                        graph.link({
                            entity: 'entity',
                            fromNode: '4',
                            toNode: '3'
                        })
                    )
                    .subscribe(null, null, done);
            });

            after(done => {
                rx.forkJoin(
                        graph.deleteByNode({
                            fromNode: '0'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '1'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '2'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '3'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '4'
                        })
                        .pipe(
                            rxop.toArray()
                        )
                    )
                    .subscribe(null, null, done);
            });

            beforeEach(() => {
                sinon.spy(graph, 'closest');
            });

            afterEach(() => {
                graph.closest.restore();
            });

            it('should traverse once', done => {
                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '1'
                            }],
                            toNode: '1'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }],
                            toNode: '2'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }],
                            toNode: '4'
                        }]);
                    }, null, done);
            });

            it('should traverse twice', done => {
                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '1'
                            }],
                            toNode: '1'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }],
                            toNode: '2'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }],
                            toNode: '4'
                        }, {
                            distance: 1.999999999999997,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }, {
                                distance: 0.999999999999998,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }, {
                            distance: 1.999999999999998,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }]);
                    }, null, done);
            });

            it('should traverse with filter', done => {
                graph.traverse({
                        filter: '3+',
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 1.999999999999997,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }, {
                                distance: 0.999999999999998,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }, {
                            distance: 1.999999999999998,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }]);
                    }, null, done);
            });
            
            it('should traverse filtering minPath', done => {
                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }],
                        minPath: 3
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 1.999999999999997,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }, {
                                distance: 0.999999999999998,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }, {
                            distance: 1.999999999999998,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }]);
                    }, null, done);
            });
            
            it('should traverse filtering modPath', done => {
                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }],
                        modPath: 3
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 1.999999999999997,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }, {
                                distance: 0.999999999999998,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }, {
                            distance: 1.999999999999998,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }]);
                    }, null, done);
            });

            it('should traverse filtering minPath, modPath and maxPath', done => {
                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }],
                        minPath: 2,
                        modPath: 2,
                        maxPath: 2
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '1'
                            }],
                            toNode: '1'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }],
                            toNode: '2'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }],
                            toNode: '4'
                        }]);
                    }, null, done);
            });

            it('should not traverse same edge more than once', done => {
                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }, {
                            entity: 'entity'
                        }]
                    })
                    .subscribe(response => {
                        expect(graph.closest.callCount).to.equal(6);

                        expect(graph.closest.getCall(0).args[0].fromNode).to.equal('0'); // 0
                        expect(graph.closest.getCall(1).args[0].fromNode).to.equal('1'); // 0 - 1
                        expect(graph.closest.getCall(2).args[0].fromNode).to.equal('2'); // 0 - 2
                        expect(graph.closest.getCall(3).args[0].fromNode).to.equal('4'); // 0 - 4
                        expect(graph.closest.getCall(4).args[0].fromNode).to.equal('3'); // 4 -3
                        expect(graph.closest.getCall(5).args[0].fromNode).to.equal('3'); // 2 - 3
                    }, null, done);
            });

            it('should traverse using remote closest', done => {
                const remoteClosest = sinon.spy(args => {
                    return graph.closest(args);
                });

                graph.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }],
                        remoteClosest
                    })
                    .subscribe(response => {
                        expect(remoteClosest).to.have.been.calledThrice;
                    }, null, done);
            });
        });

        describe('with direction', () => {
            before(done => {
                rx.forkJoin(
                        graph.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '1'
                        }),
                        graph.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '2'
                        }),
                        graph.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '4'
                        }),
                        graph.link({
                            absoluteDistance: 0.999999999999998,
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '2',
                            toNode: '3'
                        }),
                        graph.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '4',
                            toNode: '3'
                        })
                    )
                    .subscribe(null, null, done);
            });

            after(done => {
                rx.forkJoin(
                        graph.deleteByNode({
                            fromNode: '0'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '1'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '2'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '3'
                        })
                        .pipe(
                            rxop.toArray()
                        ),
                        graph.deleteByNode({
                            fromNode: '4'
                        })
                        .pipe(
                            rxop.toArray()
                        )
                    )
                    .subscribe(null, null, done);
            });

            beforeEach(() => {
                sinon.spy(graph, 'closest');
            });

            afterEach(() => {
                graph.closest.restore();
            });

            it('should traverse once', done => {
                graph.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '1'
                            }],
                            toNode: '1'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }],
                            toNode: '2'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }],
                            toNode: '4'
                        }]);
                    }, null, done);
            });

            it('should traverse twice', done => {
                graph.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            direction: 'OUT',
                            entity: 'entity'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '1'
                            }],
                            toNode: '1'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }],
                            toNode: '2'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }],
                            toNode: '4'
                        }, {
                            distance: 1.999999999999997,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }, {
                                distance: 0.999999999999998,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }, {
                            distance: 1.999999999999998,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }]);
                    }, null, done);
            });

            it('should traverse filtering minPath', done => {
                graph.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            direction: 'OUT',
                            entity: 'entity'
                        }],
                        minPath: 3
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 1.999999999999997,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }, {
                                distance: 0.999999999999998,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }, {
                            distance: 1.999999999999998,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '3'
                            }],
                            toNode: '3'
                        }]);
                    }, null, done);
            });

            it('should traverse filtering minPath and maxPath', done => {
                graph.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            direction: 'OUT',
                            entity: 'entity'
                        }],
                        minPath: 2,
                        maxPath: 2
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal([{
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '1'
                            }],
                            toNode: '1'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '2'
                            }],
                            toNode: '2'
                        }, {
                            distance: 0.999999999999999,
                            fromNode: '0',
                            path: [{
                                node: '0'
                            }, {
                                distance: 0.999999999999999,
                                entity: 'entity',
                                node: '4'
                            }],
                            toNode: '4'
                        }]);
                    }, null, done);
            });

            it('should not traverse same edge more than once', done => {
                graph.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            direction: 'OUT',
                            entity: 'entity'
                        }, {
                            direction: 'OUT',
                            entity: 'entity'
                        }]
                    })
                    .subscribe(response => {
                        expect(graph.closest.callCount).to.equal(6);

                        expect(graph.closest.getCall(0).args[0].fromNode).to.equal('0'); // 0
                        expect(graph.closest.getCall(1).args[0].fromNode).to.equal('1'); // 0 - 1
                        expect(graph.closest.getCall(2).args[0].fromNode).to.equal('2'); // 0 - 2
                        expect(graph.closest.getCall(3).args[0].fromNode).to.equal('4'); // 0 - 4
                        expect(graph.closest.getCall(4).args[0].fromNode).to.equal('3'); // 4 -3
                        expect(graph.closest.getCall(5).args[0].fromNode).to.equal('3'); // 2 - 3
                    }, null, done);
            });

            it('should traverse using remote closest', done => {
                const remoteClosest = sinon.spy(args => {
                    return graph.closest(args);
                });

                graph.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            direction: 'OUT',
                            entity: 'entity'
                        }],
                        remoteClosest
                    })
                    .subscribe(response => {
                        expect(remoteClosest).to.have.been.calledThrice;
                    }, null, done);
            });
        });
    });
});