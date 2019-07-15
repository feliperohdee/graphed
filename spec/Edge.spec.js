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
    Edge
} = require('../');

chai.use(chaiSubset);
chai.use(sinonChai);

const expect = chai.expect;

describe('Edge.js', () => {
    let edge;

    before(() => {
        edge = new Edge({
            namespace: app.namespace,
            store: app.store
        });
    });

    beforeEach(() => {
        edge = new Edge({
            namespace: app.namespace,
            store: app.store
        });
    });

    describe('constructor', () => {
        it('should throw if invalid', () => {
            expect(() => new Edge()).to.throw('namespace, store are missing or wrong.');
        });

        it('should throw if defaultDirection wrong', () => {
            expect(() => new Edge({
                defaultDirection: 'OTHER',
                namespace: app.namespace,
                store: app.store
            })).to.throw('defaultDirection should be "IN" or "OUT", not "OTHER".');
        });

        it('should have decrementPath by default', () => {
            expect(edge.decrementPath).to.equal(1 / (10 ** 15));
        });

        it('should have custom decrementPath', () => {
            edge = new Edge({
                decrementPath: 10,
                namespace: app.namespace,
                store: app.store
            });

            expect(edge.decrementPath).to.equal(10);
        });

        it('should defaultDirection be undefined by default', () => {
            expect(edge.defaultDirection).to.be.undefined;
        });

        it('should have custom defaultDirection', () => {
            edge = new Edge({
                defaultDirection: 'IN',
                namespace: app.namespace,
                store: app.store
            });

            expect(edge.defaultDirection).to.equal('IN');
        });

        it('should defaultEntity be undefined by default', () => {
            expect(edge.defaultEntity).to.be.undefined;
        });

        it('should have custom defaultEntity', () => {
            edge = new Edge({
                defaultEntity: 'entity',
                namespace: app.namespace,
                store: app.store
            });

            expect(edge.defaultEntity).to.equal('entity');
        });

        it('should have namespace', () => {
            expect(edge.namespace).to.be.a('string');
        });

        it('should node be undefined by default', () => {
            expect(edge.node).to.be.undefined;
        });

        it('should have store', () => {
            expect(edge.store).to.be.an('object');
        });

        it('should validate store', () => {
            expect(() => new Edge({
                namespace: app.namespace,
                store: _.omit(app.store, ['countEdges', 'deleteEdge'])
            })).to.throw('Invalid store, missing countEdges, deleteEdge');
        });

        it('should have store based functions', () => {
            expect(edge.countEdges).not.to.be.undefined;
            expect(edge.deleteEdge).not.to.be.undefined;
            expect(edge.deleteEdges).not.to.be.undefined;
            expect(edge.getAll).not.to.be.undefined;
            expect(edge.getAllByDistance).not.to.be.undefined;
            expect(edge.incrementEdge).not.to.be.undefined;
            expect(edge.setEdge).not.to.be.undefined;
        });
    });

    describe('allAll', () => {
		afterEach(done => {
			Observable.forkJoin(
					edge.deleteByNode({
						fromNode: '0'
					})
					.toArray(),
					edge.deleteByNode({
						fromNode: '1'
					})
					.toArray(),
					edge.deleteByNode({
						fromNode: '2'
					})
					.toArray(),
					edge.deleteByNode({
						fromNode: '3'
					})
					.toArray()
				)
				.subscribe(null, null, done);
		});

		it('should throw if wrong direction', done => {
			edge.allAll({
					direction: 'OTHER'
				})
				.subscribe(null, err => {
					expect(err.message).to.equal('direction should be "IN" or "OUT", not "OTHER".');
					done();
				});
		});

		it('should throw if wrong distance', done => {
			edge.allAll({
					distance: 'string'
				})
				.subscribe(null, err => {
					expect(err.message).to.equal('distance should be a number or function with signature "(collectionSize, index): number".');
					done();
				});
		});

		it('should throw if no entity', done => {
			edge.allAll()
				.subscribe(null, err => {
					expect(err.message).to.equal('entity is missing.');
					done();
				});
		});

		it('should add multiple edges', done => {
			edge.allAll({
					collection: ['0', '1', '2', '3'],
					distance: (collectionSize, fromNodeIndex, toNodeIndex) => {
						return collectionSize - Math.abs(fromNodeIndex - toNodeIndex);
					},
					entity: 'entity'
				})
				.toArray()
				.subscribe(response => {
					expect(_.size(response)).to.equal(6);

					expect(response[0][0]).to.deep.contain({
						fromNode: '0',
						distance: 0.999999999999997,
						toNode: '1'
					});

					expect(response[1][0]).to.deep.contain({
						fromNode: '0',
						distance: 0.999999999999998,
						toNode: '2'
					});

					expect(response[2][0]).to.deep.contain({
						fromNode: '0',
						distance: 0.999999999999999,
						toNode: '3'
					});

					expect(response[3][0]).to.deep.contain({
						fromNode: '1',
						distance: 0.999999999999997,
						toNode: '2'
					});

					expect(response[4][0]).to.deep.contain({
						fromNode: '1',
						distance: 0.999999999999998,
						toNode: '3'
					});

					expect(response[5][0]).to.deep.contain({
						fromNode: '2',
						distance: 0.999999999999997,
						toNode: '3'
					});
				}, null, done);
		});

		it('should add multiple edges with direction', done => {
			edge.allAll({
					collection: ['0', '1', '2', '3'],
					direction: 'OUT',
					distance: (collectionSize, fromNodeIndex, toNodeIndex) => {
						return collectionSize - Math.abs(fromNodeIndex - toNodeIndex);
					},
					entity: 'entity'
				})
				.toArray()
				.subscribe(response => {
					expect(_.size(response)).to.equal(12);

					expect(response[0][0]).to.deep.contain({
						fromNode: '0',
						direction: 'OUT',
						distance: 0.999999999999997,
						toNode: '1'
					});

					expect(response[0][1]).to.deep.contain({
						fromNode: '1',
						direction: 'IN',
						distance: 0.999999999999997,
						toNode: '0'
					});

					expect(response[1][0]).to.deep.contain({
						fromNode: '0',
						direction: 'OUT',
						distance: 0.999999999999998,
						toNode: '2'
					});

					expect(response[1][1]).to.deep.contain({
						fromNode: '2',
						direction: 'IN',
						distance: 0.999999999999998,
						toNode: '0'
					});

					expect(response[2][0]).to.deep.contain({
						fromNode: '0',
						direction: 'OUT',
						distance: 0.999999999999999,
						toNode: '3'
					});

					expect(response[2][1]).to.deep.contain({
						fromNode: '3',
						direction: 'IN',
						distance: 0.999999999999999,
						toNode: '0'
					});

					// 
					expect(response[3][0]).to.deep.contain({
						fromNode: '1',
						direction: 'OUT',
						distance: 0.999999999999997,
						toNode: '0'
					});

					expect(response[3][1]).to.deep.contain({
						fromNode: '0',
						direction: 'IN',
						distance: 0.999999999999997,
						toNode: '1'
					});

					expect(response[4][0]).to.deep.contain({
						fromNode: '1',
						direction: 'OUT',
						distance: 0.999999999999997,
						toNode: '2'
					});

					expect(response[4][1]).to.deep.contain({
						fromNode: '2',
						direction: 'IN',
						distance: 0.999999999999997,
						toNode: '1'
					});

					expect(response[5][0]).to.deep.contain({
						fromNode: '1',
						direction: 'OUT',
						distance: 0.999999999999998,
						toNode: '3'
					});

					expect(response[5][1]).to.deep.contain({
						fromNode: '3',
						direction: 'IN',
						distance: 0.999999999999998,
						toNode: '1'
					});

					// 
					expect(response[6][0]).to.deep.contain({
						fromNode: '2',
						direction: 'OUT',
						distance: 0.999999999999998,
						toNode: '0'
					});

					expect(response[6][1]).to.deep.contain({
						fromNode: '0',
						direction: 'IN',
						distance: 0.999999999999998,
						toNode: '2'
					});

					expect(response[7][0]).to.deep.contain({
						fromNode: '2',
						direction: 'OUT',
						distance: 0.999999999999997,
						toNode: '1'
					});

					expect(response[7][1]).to.deep.contain({
						fromNode: '1',
						direction: 'IN',
						distance: 0.999999999999997,
						toNode: '2'
					});

					expect(response[8][0]).to.deep.contain({
						fromNode: '2',
						direction: 'OUT',
						distance: 0.999999999999997,
						toNode: '3'
					});

					expect(response[8][1]).to.deep.contain({
						fromNode: '3',
						direction: 'IN',
						distance: 0.999999999999997,
						toNode: '2'
					});

					// 
					expect(response[9][0]).to.deep.contain({
						fromNode: '3',
						direction: 'OUT',
						distance: 0.999999999999999,
						toNode: '0'
					});

					expect(response[9][1]).to.deep.contain({
						fromNode: '0',
						direction: 'IN',
						distance: 0.999999999999999,
						toNode: '3'
					});

					expect(response[10][0]).to.deep.contain({
						fromNode: '3',
						direction: 'OUT',
						distance: 0.999999999999998,
						toNode: '1'
					});

					expect(response[10][1]).to.deep.contain({
						fromNode: '1',
						direction: 'IN',
						distance: 0.999999999999998,
						toNode: '3'
					});

					expect(response[11][0]).to.deep.contain({
						fromNode: '3',
						direction: 'OUT',
						distance: 0.999999999999997,
						toNode: '2'
					});

					expect(response[11][1]).to.deep.contain({
						fromNode: '2',
						direction: 'IN',
						distance: 0.999999999999997,
						toNode: '3'
					});
				}, null, done);
		});
	});

    describe('count', () => {
        before(done => {
            Observable.forkJoin(
                    edge.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    edge.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '3'
                    }),
                    edge.link({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '3'
                    })
                    .toArray()
                )
                .subscribe(null, null, done);
        });

        it('should throw if wrong direction', done => {
            edge.count({
                    direction: 'OTHER'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('direction should be "IN" or "OUT", not "OTHER".');
                    done();
                });
        });

        it('should throw if no entity', done => {
            edge.count()
                .subscribe(null, err => {
                    expect(err.message).to.equal('entity is missing.');
                    done();
                });
        });

        it('should throw if no fromNode', done => {
            edge.count({
                    entity: 'entity'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        it('should return elements count', done => {
            Observable.forkJoin(
                    edge.count({
                        entity: 'entity',
                        fromNode: '1'
                    }),
                    edge.count({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '1'
                    }),
                    edge.count({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '1'
                    }),
                    edge.count({
                        entity: 'entity',
                        fromNode: '2'
                    }),
                    edge.count({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2'
                    }),
                    edge.count({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '2'
                    }),
                    edge.count({
                        entity: 'entity',
                        fromNode: '3'
                    }),
                    edge.count({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '3'
                    }),
                    edge.count({
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

    describe('delete', () => {
        beforeEach(done => {
            Observable.forkJoin(
                    edge.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    edge.link({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(null, null, done);
        });

        it('should throw if wrong direction', done => {
            edge.delete({
                    direction: 'OTHER'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('direction should be "IN" or "OUT", not "OTHER".');
                    done();
                });
        });

        it('should throw if no entity', done => {
            edge.delete()
                .subscribe(null, err => {
                    expect(err.message).to.equal('entity is missing.');
                    done();
                });
        });

        it('should throw if no fromNode', done => {
            edge.delete({
                    entity: 'entity'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        it('should throw if no toNode', done => {
            edge.delete({
                    entity: 'entity',
                    fromNode: '1'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('toNode is missing.');
                    done();
                });
        });

        describe('with direction', () => {
            it('should delete a edge', done => {
                edge.delete({
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
                edge.delete({
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
                edge.delete({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    })
                    .subscribe(response => {
                        expect(response.fromNode).to.deep.equal('1');
                    }, null, done);
            });

            it('should delete two edges', done => {
                edge.delete({
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
            Observable.forkJoin(
                    edge.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    edge.link({
                        entity: 'entity',
                        direction: 'IN',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(null, null, done);
        });

        it('should throw if no fromNode', done => {
            edge.deleteByNode()
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        it('should delete by fromNode', done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(([
                    fromNode1,
                    fromNode2
                ]) => {
                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(fromNode2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '3'
                    }]);

                    expect(fromNode2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '3',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should delete by fromNode and entity', done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1',
                        entity: 'entity'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2',
                        entity: 'inexistent'
                    })
                    .toArray()
                )
                .subscribe(([
                    fromNode1,
                    fromNode2
                ]) => {
                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(fromNode1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(fromNode2).to.deep.equal([]);
                }, null, done);
        });

        it('should delete by fromNode, entity and direction', done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1',
                        entity: 'entity',
                        direction: 'IN'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2',
                        entity: 'entity',
                        direction: 'IN'
                    })
                    .toArray()
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
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(fromNode2).not.to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(fromNode2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '3'
                    }]);

                    expect(fromNode2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '3',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });
    });

    describe('allByNode', () => {
        before(done => {
            Observable.forkJoin(
                    edge.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2',
                    }),
                    edge.link({
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        toNode: '3'
                    })
                )
                .subscribe(null, null, done);
        });

        after(done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(null, null, done);
        });

        it('should throw if no fromNode', done => {
            edge.allByNode()
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        it('should fetch all by node', done => {
            Observable.forkJoin(
                    edge.allByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.allByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '3'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '3',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should fetch all by node only by entity and direction', done => {
            Observable.forkJoin(
                    edge.allByNode({
                        fromNode: '1',
                        direction: 'OUT',
                        entity: 'entity'
                    })
                    .toArray(),
                    edge.allByNode({
                        fromNode: '2',
                        direction: 'OUT',
                        entity: 'entity'
                    })
                    .toArray()
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
                        namespace: 'spec',
                        toNode: '3'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '3',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should fetch all by node only by distance', done => {
            Observable.forkJoin(
                    edge.allByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.allByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '3'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'IN',
                        entity: 'entity',
                        fromNode: '3',
                        namespace: 'spec',
                        toNode: '2'
                    }]);
                }, null, done);
        });

        it('should fetch all by node without inverse', done => {
            Observable.forkJoin(
                    edge.allByNode({
                        fromNode: '1',
                        noInverse: true
                    })
                    .toArray(),
                    edge.allByNode({
                        fromNode: '2',
                        noInverse: true
                    })
                    .toArray()
                )
                .subscribe(([
                    operation1,
                    operation2
                ]) => {
                    expect(operation1).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '1',
                        namespace: 'spec',
                        toNode: '2'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: null,
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '1'
                    }]);

                    expect(operation2).to.containSubset([{
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2',
                        namespace: 'spec',
                        toNode: '3'
                    }]);
                }, null, done);
        });

        it('should fetch all by node returning only nodes', done => {
            Observable.forkJoin(
                    edge.allByNode({
                        fromNode: '1',
                        onlyNodes: true
                    })
                    .toArray(),
                    edge.allByNode({
                        fromNode: '2',
                        noInverse: true,
                        onlyNodes: true
                    })
                    .toArray()
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
            Observable.forkJoin(
                    edge.link({
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '2'
                    }),
                    edge.link({
                        absoluteDistance: 0.999999999999998,
                        entity: 'entity',
                        fromNode: '1',
                        toNode: '3'
                    }),
                    edge.link({
                        entity: 'entity',
                        direction: 'OUT',
                        fromNode: '2',
                        toNode: '3'
                    }),
                    edge.link({
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
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '3'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '4'
                    })
                    .toArray()
                )
                .subscribe(null, null, done);
        });

        it('should throw if wrong direction', done => {
            edge.closest({
                    direction: 'OTHER'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('direction should be "IN" or "OUT", not "OTHER".');
                    done();
                });
        });

        it('should throw if no entity', done => {
            edge.closest()
                .subscribe(null, err => {
                    expect(err.message).to.equal('entity is missing.');
                    done();
                });
        });

        it('should throw if no fromNode', done => {
            edge.closest({
                    entity: 'entity'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        describe('by distance', () => {
            it('should get closest nodes', done => {
                edge.closest({
                        entity: 'entity',
                        fromNode: '1'
                    })
                    .toArray()
                    .subscribe(response => {
                        expect(response[0]).to.deep.equal({
                            direction: null,
                            distance: 0.999999999999998,
                            entity: 'entity',
                            fromNode: '1',
                            namespace: app.namespace,
                            toNode: '3'
                        });

                        expect(response[1]).to.deep.equal({
                            direction: null,
                            distance: 0.999999999999999,
                            entity: 'entity',
                            fromNode: '1',
                            namespace: app.namespace,
                            toNode: '2'
                        });
                    }, null, done);
            });

            it('should get closest nodes with direction', done => {
                edge.closest({
                        direction: 'OUT',
                        entity: 'entity',
                        fromNode: '2'
                    })
                    .toArray()
                    .subscribe(response => {
                        expect(response[0]).to.deep.equal({
                            direction: 'OUT',
                            distance: 0.999999999999998,
                            entity: 'entity',
                            fromNode: '2',
                            namespace: app.namespace,
                            toNode: '4'
                        });

                        expect(response[1]).to.deep.equal({
                            direction: 'OUT',
                            distance: 0.999999999999999,
                            entity: 'entity',
                            fromNode: '2',
                            namespace: app.namespace,
                            toNode: '3'
                        });
                    }, null, done);
            });

            it('should get closest nodes desc', done => {
                edge.closest({
                        desc: true,
                        entity: 'entity',
                        fromNode: '1'
                    })
                    .toArray()
                    .subscribe(response => {
                        expect(response[1]).to.deep.equal({
                            direction: null,
                            distance: 0.999999999999998,
                            entity: 'entity',
                            fromNode: '1',
                            namespace: app.namespace,
                            toNode: '3'
                        });

                        expect(response[0]).to.deep.equal({
                            direction: null,
                            distance: 0.999999999999999,
                            entity: 'entity',
                            fromNode: '1',
                            namespace: app.namespace,
                            toNode: '2'
                        });
                    }, null, done);
            });
        });
    });

    describe('link', () => {
        afterEach(done => {
            Observable.forkJoin(
                    edge.deleteByNode({
                        fromNode: '1'
                    })
                    .toArray(),
                    edge.deleteByNode({
                        fromNode: '2'
                    })
                    .toArray()
                )
                .subscribe(null, null, done);
        });

        it('should throw if wrong absoluteDistance', done => {
            edge.link({
                    absoluteDistance: 'string'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('absoluteDistance should be a number.');
                    done();
                });
        });

        it('should throw if absoluteDistance less than 0', done => {
            edge.link({
                    absoluteDistance: -0.0000000001
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('distances should be >= 0.');
                    done();
                });
        });

        it('should throw if wrong direction', done => {
            edge.link({
                    direction: 'OTHER'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('direction should be "IN" or "OUT", not "OTHER".');
                    done();
                });
        });

        it('should throw if wrong distance', done => {
            edge.link({
                    distance: 'string',
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('distance should be a number.');
                    done();
                });
        });

        it('should throw if no entity', done => {
            edge.link()
                .subscribe(null, err => {
                    expect(err.message).to.equal('entity is missing.');
                    done();
                });
        });

        it('should throw if no fromNode', done => {
            edge.link({
                    entity: 'entity'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        it('should throw if no toNode', done => {
            edge.link({
                    entity: 'entity',
                    fromNode: '1'
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('toNode is missing.');
                    done();
                });
        });

        it('should insert two edges with absoluteDistance', done => {
            edge.link({
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
            edge.link({
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
            edge.link({
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
            edge.link({
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
            edge.link({
                    distance: 5,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .mergeMap(() => edge.link({
                    distance: 0,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                }))
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
            edge.decrementPath = 0.1;
            edge.link({
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
            edge.link({
                    distance: -1,
                    entity: 'entity',
                    fromNode: '1',
                    toNode: '2'
                })
                .subscribe(response => {
                    const [
                        firstEdge,
                        secondEdge
                    ] = response;

                    expect(firstEdge.distance).to.equal(1.000000000000001);
                    expect(secondEdge.distance).to.equal(1.000000000000001);
                }, null, done);
        });

        describe('without direction', () => {
            it('should insert two edges', done => {
                edge.link({
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
                edge.link({
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
    });

    describe('traverse', () => {
        it('should return empty if no jobs', done => {
            edge.traverse()
                .subscribe(response => {
                    expect(response).to.deep.equal([]);
                }, null, done);
        });

        it('should handle errors', done => {
            edge.traverse({
                    jobs: [{
                        entity: 'entity'
                    }]
                })
                .subscribe(null, err => {
                    expect(err.message).to.equal('fromNode is missing.');
                    done();
                });
        });

        describe('without direction', () => {
            before(done => {
                Observable.forkJoin(
                        edge.link({
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '1'
                        }),
                        edge.link({
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '2'
                        }),
                        edge.link({
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '4'
                        }),
                        edge.link({
                            absoluteDistance: 0.999999999999998,
                            entity: 'entity',
                            fromNode: '2',
                            toNode: '3'
                        }),
                        edge.link({
                            entity: 'entity',
                            fromNode: '4',
                            toNode: '3'
                        })
                    )
                    .subscribe(null, null, done);
            });

            after(done => {
                Observable.forkJoin(
                        edge.deleteByNode({
                            fromNode: '0'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '1'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '2'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '3'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '4'
                        })
                        .toArray()
                    )
                    .subscribe(null, null, done);
            });

            beforeEach(() => {
                sinon.spy(edge, 'closest');
            });

            afterEach(() => {
                edge.closest.restore();
            });

            it('should traverse once', done => {
                edge.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 1,
                                    '4': 1
                                },
                                entity: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 1,
                                    '4': 1
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should traverse twice', done => {
                edge.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 2,
                                    '3': 2,
                                    '4': 2
                                },
                                entity: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 2,
                                    '3': 2,
                                    '4': 2
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should traverse filtering minPath', done => {
                edge.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }],
                        minPath: 3
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 2,
                                    '3': 2,
                                    '4': 2
                                },
                                entity: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 2,
                                    '3': 2,
                                    '4': 2
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should traverse filtering minPath and maxPath', done => {
                edge.traverse({
                        jobs: [{
                            entity: 'entity',
                            fromNode: '0'
                        }, {
                            entity: 'entity'
                        }],
                        minPath: 2,
                        maxPath: 2
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 2,
                                    '3': 2,
                                    '4': 2
                                },
                                entity: {
                                    '0': 3,
                                    '1': 1,
                                    '2': 2,
                                    '3': 2,
                                    '4': 2
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should not traverse same edge more than once', done => {
                edge.traverse({
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
                        expect(edge.closest.callCount).to.equal(6);

                        expect(edge.closest.getCall(0).args[0].fromNode).to.equal('0'); // 0
                        expect(edge.closest.getCall(1).args[0].fromNode).to.equal('1'); // 0 - 1
                        expect(edge.closest.getCall(2).args[0].fromNode).to.equal('2'); // 0 - 2
                        expect(edge.closest.getCall(3).args[0].fromNode).to.equal('4'); // 0 - 4
                        expect(edge.closest.getCall(4).args[0].fromNode).to.equal('3'); // 4 -3
                        expect(edge.closest.getCall(5).args[0].fromNode).to.equal('3'); // 2 - 3
                    }, null, done);
            });

            it('should traverse using remote closest', done => {
                const remoteClosest = sinon.spy(args => {
                    return edge.closest(args);
                });

                edge.traverse({
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
                Observable.forkJoin(
                        edge.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '1'
                        }),
                        edge.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '2'
                        }),
                        edge.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0',
                            toNode: '4'
                        }),
                        edge.link({
                            absoluteDistance: 0.999999999999998,
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '2',
                            toNode: '3'
                        }),
                        edge.link({
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '4',
                            toNode: '3'
                        })
                    )
                    .subscribe(null, null, done);
            });

            after(done => {
                Observable.forkJoin(
                        edge.deleteByNode({
                            fromNode: '0'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '1'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '2'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '3'
                        })
                        .toArray(),
                        edge.deleteByNode({
                            fromNode: '4'
                        })
                        .toArray()
                    )
                    .subscribe(null, null, done);
            });

            beforeEach(() => {
                sinon.spy(edge, 'closest');
            });

            afterEach(() => {
                edge.closest.restore();
            });

            it('should traverse once', done => {
                edge.traverse({
                        jobs: [{
                            direction: 'OUT',
                            entity: 'entity',
                            fromNode: '0'
                        }]
                    })
                    .subscribe(response => {
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '1': 1,
                                    '2': 1,
                                    '4': 1
                                },
                                entity: {
                                    '1': 1,
                                    '2': 1,
                                    '4': 1
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should traverse twice', done => {
                edge.traverse({
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
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '1': 1,
                                    '2': 1,
                                    '3': 2,
                                    '4': 1
                                },
                                entity: {
                                    '1': 1,
                                    '2': 1,
                                    '3': 2,
                                    '4': 1
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should traverse filtering minPath', done => {
                edge.traverse({
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
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '1': 1,
                                    '2': 1,
                                    '3': 2,
                                    '4': 1
                                },
                                entity: {
                                    '1': 1,
                                    '2': 1,
                                    '3': 2,
                                    '4': 1
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should traverse filtering minPath and maxPath', done => {
                edge.traverse({
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
                        expect(response).to.deep.equal({
                            frequency: {
                                all: {
                                    '1': 1,
                                    '2': 1,
                                    '3': 2,
                                    '4': 1
                                },
                                entity: {
                                    '1': 1,
                                    '2': 1,
                                    '3': 2,
                                    '4': 1
                                }
                            },
                            paths: [{
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
                            }]
                        });
                    }, null, done);
            });

            it('should not traverse same edge more than once', done => {
                edge.traverse({
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
                        expect(edge.closest.callCount).to.equal(6);

                        expect(edge.closest.getCall(0).args[0].fromNode).to.equal('0'); // 0
                        expect(edge.closest.getCall(1).args[0].fromNode).to.equal('1'); // 0 - 1
                        expect(edge.closest.getCall(2).args[0].fromNode).to.equal('2'); // 0 - 2
                        expect(edge.closest.getCall(3).args[0].fromNode).to.equal('4'); // 0 - 4
                        expect(edge.closest.getCall(4).args[0].fromNode).to.equal('3'); // 4 -3
                        expect(edge.closest.getCall(5).args[0].fromNode).to.equal('3'); // 2 - 3
                    }, null, done);
            });

            it('should traverse using remote closest', done => {
                const remoteClosest = sinon.spy(args => {
                    return edge.closest(args);
                });

                edge.traverse({
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