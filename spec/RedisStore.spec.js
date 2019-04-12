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

describe.only('RedisStore.js', () => {
	beforeEach(done => {
		Observable.forkJoin(
				store.setEdgeByDistance({
					distance: 1,
					entity: 'entity',
					fromNode: '0',
					namespace: app.namespace,
					toNode: '1'
				}),
				store.setEdgeByDistance({
					distance: 1,
					entity: 'entity',
					fromNode: '0',
					namespace: app.namespace,
					toNode: '2'
				}),
				store.setEdgeByDistance({
					distance: 1,
					direction: 'OUT',
					entity: 'entity-2',
					fromNode: '1',
					namespace: app.namespace,
					toNode: '2'
				}),
				store.setEdgeByTimestamp({
					timestamp: _.now(),
					entity: 'entity',
					fromNode: '0',
					namespace: app.namespace,
					toNode: '1'
				}),
				store.setEdgeByTimestamp({
					timestamp: _.now(),
					entity: 'entity',
					fromNode: '0',
					namespace: app.namespace,
					toNode: '2'
				}),
				store.setEdgeByTimestamp({
					timestamp: _.now(),
					direction: 'OUT',
					entity: 'entity-2',
					fromNode: '1',
					namespace: app.namespace,
					toNode: '2'
				}),
				store.setNodes({
					namespace: app.namespace,
					values: [{
						data: {
							a: 1
						},
						id: '0',
					}, {
						data: {
							a: 1
						},
						id: '1',
					}]
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
				.toArray(),
				store.deleteNode({
					id: '0',
					namespace: app.namespace
				})
				.toArray(),
				store.deleteNode({
					id: '1',
					namespace: app.namespace
				})
				.toArray(),
				store.deleteNode({
					id: '2',
					namespace: app.namespace
				})
				.toArray(),
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
				namespace: app.namespace,
				type: 'byTimestamp'
			})).to.equal('graph-1:e.t');

			expect(store._composeId({
				fromNode: 'fromNode',
				namespace: app.namespace,
				type: 'byTimestamp'
			})).to.equal('graph-1:e.t:fromNode');

			expect(store._composeId({
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: app.namespace,
				type: 'byTimestamp'
			})).to.equal('graph-1:e.t:fromNode:entity');

			expect(store._composeId({
				direction: 'direction',
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: app.namespace,
				type: 'byTimestamp'
			})).to.equal('graph-1:e.t:fromNode:entity:direction');

			expect(store._composeId({
				namespace: app.namespace,
				type: 'byDistance'
			})).to.equal('graph-1:e.d');

			expect(store._composeId({
				fromNode: 'fromNode',
				namespace: app.namespace,
				type: 'byDistance'
			})).to.equal('graph-1:e.d:fromNode');

			expect(store._composeId({
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: app.namespace,
				type: 'byDistance'
			})).to.equal('graph-1:e.d:fromNode:entity');

			expect(store._composeId({
				direction: 'direction',
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: app.namespace,
				type: 'byDistance'
			})).to.equal('graph-1:e.d:fromNode:entity:direction');
		});
	});

	describe('_parseId', () => {
		it('should parse id', () => {
			expect(store._parseId('graph-1:e.t:fromNode')).to.deep.equal({
				direction: null,
				entity: undefined,
				fromNode: 'fromNode',
				namespace: 'graph-1',
				type: 'byTimestamp'
			});

			expect(store._parseId('graph-1:e.d:fromNode')).to.deep.equal({
				direction: null,
				entity: undefined,
				fromNode: 'fromNode',
				namespace: 'graph-1',
				type: 'byDistance'
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
					expect(response.length).to.equal(10);

					// by distance
					expect(response).to.deep.include('graph-1:e.d:0:entity');
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.d:1:entity');
					expect(response).to.deep.include('graph-1:e.d:2:entity-2:IN');
					expect(response).to.deep.include('graph-1:e.d:2:entity');
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:0:entity');
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.t:1:entity');
					expect(response).to.deep.include('graph-1:e.t:2:entity-2:IN');
					expect(response).to.deep.include('graph-1:e.t:2:entity');
				}, null, done);
		});

		it('should get edge keys by fromNode', done => {
			store._getEdgesKeys({
					fromNode: '1',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(4);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.d:1:entity');
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.t:1:entity');
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
					expect(response.length).to.equal(2);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
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
					expect(response.length).to.equal(2);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
				}, null, done);
		});
	});

	describe('_getEdgesKeysByDistance', () => {
		it('should throw if invalid', () => {
			expect(() => store._getEdgesKeysByDistance()).to.throw('namespace is missing or wrong.');
		});

		it('should get edge keys', done => {
			store._getEdgesKeysByDistance({
					namespace: app.namespace,
					type: 'byTimestamp' // false
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(5);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:0:entity');
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.d:1:entity');
					expect(response).to.deep.include('graph-1:e.d:2:entity-2:IN');
					expect(response).to.deep.include('graph-1:e.d:2:entity');
				}, null, done);
		});

		it('should get edge keys by fromNode', done => {
			store._getEdgesKeysByDistance({
					fromNode: '1',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(2);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.d:1:entity');
				}, null, done);
		});

		it('should get edge keys by fromNode and entity', done => {
			store._getEdgesKeysByDistance({
					fromNode: '1',
					entity: 'entity-2',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(1);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
				}, null, done);
		});

		it('should get edge keys by fromNode, entity and direction', done => {
			store._getEdgesKeysByDistance({
					fromNode: '1',
					entity: 'entity-2',
					direction: 'OUT',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(1);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:1:entity-2:OUT');
				}, null, done);
		});
	});

	describe('_getEdgesKeysByTimestamp', () => {
		it('should throw if invalid', () => {
			expect(() => store._getEdgesKeysByTimestamp()).to.throw('namespace is missing or wrong.');
		});

		it('should get edge keys', done => {
			store._getEdgesKeysByTimestamp({
					namespace: app.namespace,
					type: 'byTimestamp'
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(5);
					// by distance
					expect(response).to.deep.include('graph-1:e.t:0:entity');
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.t:1:entity');
					expect(response).to.deep.include('graph-1:e.t:2:entity-2:IN');
					expect(response).to.deep.include('graph-1:e.t:2:entity');
				}, null, done);
		});

		it('should get edge keys by fromNode', done => {
			store._getEdgesKeysByTimestamp({
					fromNode: '1',
					namespace: app.namespace,
					type: 'byTimestamp'
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(2);
					// by distance
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
					expect(response).to.deep.include('graph-1:e.t:1:entity');
				}, null, done);
		});

		it('should get edge keys by fromNode and entity', done => {
			store._getEdgesKeysByTimestamp({
					fromNode: '1',
					entity: 'entity-2',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(1);
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
				}, null, done);
		});

		it('should get edge keys by fromNode, entity and direction', done => {
			store._getEdgesKeysByTimestamp({
					fromNode: '1',
					entity: 'entity-2',
					direction: 'OUT',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(1);
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:1:entity-2:OUT');
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

		it('should return deleted edges', done => {
			store.deleteEdge({
					namespace: app.namespace,
					fromNode: '0',
					entity: 'entity',
					toNode: '1'
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						direction: null,
						entity: 'entity',
						fromNode: '0',
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: null,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
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
					expect(response).to.deep.equal([null, null]);
				}, null, done);
		});

		describe('without timestamp', () => {
			beforeEach(done => {
				store.deleteEdges({
						namespace: app.namespace,
						fromNode: '0',
						entity: 'entity',
						type: 'byTimestamp'
					})
					.subscribe(null, null, done);
			});

			it('should return deleted edges', done => {
				store.deleteEdge({
						namespace: app.namespace,
						fromNode: '0',
						entity: 'entity',
						toNode: '1'
					})
					.subscribe(response => {
						expect(response).to.deep.equal([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '1'
						}, {
							direction: null,
							entity: 'entity',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '0'
						}]);
					}, null, done);
			});
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
					expect(response.length).to.equal(12);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});

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
					expect(response.length).to.equal(8);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});

					return store._getEdgesKeys({
						namespace: app.namespace
					});
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(4);
					// by distance
					expect(response).to.deep.include('graph-1:e.d:0:entity');
					expect(response).to.deep.include('graph-1:e.d:2:entity');
					// by timestamp
					expect(response).to.deep.include('graph-1:e.t:0:entity');
					expect(response).to.deep.include('graph-1:e.t:2:entity');
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
					expect(response.length).to.equal(4);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});

					return store._getEdgesKeys({
						namespace: app.namespace
					});
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(6);
					// // by distance
					expect(response).to.deep.include('graph-1:e.d:0:entity');
					expect(response).to.deep.include('graph-1:e.d:1:entity');
					expect(response).to.deep.include('graph-1:e.d:2:entity');
					// // by timestamp
					expect(response).to.deep.include('graph-1:e.t:0:entity');
					expect(response).to.deep.include('graph-1:e.t:1:entity');
					expect(response).to.deep.include('graph-1:e.t:2:entity');
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
					expect(response.length).to.equal(4);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});

					return store._getEdgesKeys({
						namespace: app.namespace
					});
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(6);

					// // by distance
					expect(response).to.deep.include('graph-1:e.d:0:entity');
					expect(response).to.deep.include('graph-1:e.d:1:entity');
					expect(response).to.deep.include('graph-1:e.d:2:entity');
					// // by timestamp
					expect(response).to.deep.include('graph-1:e.t:0:entity');
					expect(response).to.deep.include('graph-1:e.t:1:entity');
					expect(response).to.deep.include('graph-1:e.t:2:entity');
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
					const withDistance = _.filter(response, item => item.distance);
					const withTimestamp = _.filter(response, item => item.timestamp);

					expect(response.length).to.equal(12);
					expect(withDistance.length).to.equal(6);
					expect(withTimestamp.length).to.equal(6);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});
				}, null, done);
		});

		it('should get all edges by fromNode', done => {
			store.getEdges({
					fromNode: '1',
					namespace: app.namespace
				})
				.toArray()
				.subscribe(response => {
					expect(response.length).to.equal(8);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '0',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});
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
					expect(response.length).to.equal(4);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: null,
							entity: 'entity',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '0',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);
					});
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
					expect(response.length).to.equal(4);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});
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
					expect(response.length).to.equal(4);

					_.each(['byDistance', 'byTimestamp'], type => {
						expect(response).to.containSubset([{
							direction: 'OUT',
							entity: 'entity-2',
							fromNode: '1',
							namespace: 'graph-1',
							toNode: '2',
							type
						}]);

						expect(response).to.containSubset([{
							direction: 'IN',
							entity: 'entity-2',
							fromNode: '2',
							namespace: 'graph-1',
							toNode: '1',
							type
						}]);
					});
				}, null, done);
		});
	});

	describe('getEdgesByDistance', () => {
		beforeEach(done => {
			Observable.forkJoin(
					store.setEdgeByDistance({
						distance: 0.8,
						direction: 'OUT',
						entity: 'entity-2',
						fromNode: '1',
						namespace: app.namespace,
						toNode: '3'
					}),
					store.setEdgeByDistance({
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
						namespace: 'graph-1',
						toNode: '3',
						type: 'byDistance'
					}, {
						direction: 'OUT',
						distance: 0.9,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '4',
						type: 'byDistance'
					}, {
						direction: 'OUT',
						distance: 1,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '2',
						type: 'byDistance'
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
						namespace: 'graph-1',
						toNode: '2',
						type: 'byDistance'
					}, {
						direction: 'OUT',
						distance: 0.9,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '4',
						type: 'byDistance'
					}, {
						direction: 'OUT',
						distance: 0.8,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '3',
						type: 'byDistance'
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
						namespace: 'graph-1',
						toNode: '2',
						type: 'byDistance'
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
						namespace: 'graph-1',
						toNode: '3',
						type: 'byDistance'
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
						namespace: 'graph-1',
						toNode: '3',
						type: 'byDistance'
					}, {
						direction: 'OUT',
						distance: 0.9,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '4',
						type: 'byDistance'
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

	describe('getEdgesByTimestamp', () => {
		let now;

		beforeEach(done => {
			now = _.now();

			Observable.forkJoin(
					store.setEdgeByDistance({
						distance: 1000,
						direction: 'OUT',
						entity: 'entity-2',
						fromNode: '1',
						namespace: app.namespace,
						toNode: '3'
					}),
					store.setEdgeByDistance({
						distance: 1,
						direction: 'OUT',
						entity: 'entity-2',
						fromNode: '1',
						namespace: app.namespace,
						toNode: '4'
					}),
					store.setEdgeByTimestamp({
						timestamp: now - 10,
						direction: 'OUT',
						entity: 'entity-2',
						fromNode: '1',
						namespace: app.namespace,
						toNode: '3'
					}),
					store.setEdgeByTimestamp({
						timestamp: now - 5,
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
			expect(() => store.getEdgesByTimestamp()).to.throw('entity, fromNode, namespace are missing or wrong.');
		});

		it('should throw if wrong timestamp', done => {
			store.getEdgesByTimestamp({
					namespace: app.namespace,
					entity: 'entity',
					timestamp: 1,
					fromNode: '1'
				})
				.subscribe(null, err => {
					expect(err.message).to.equal('timestamp should be an array like [min?: number, max?: number].');
					done();
				});
		});

		it('should throw if wrong limit', done => {
			store.getEdgesByTimestamp({
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
			store.getEdgesByTimestamp({
					namespace: app.namespace,
					entity: 'entity-2',
					fromNode: '1',
					direction: 'OUT'
				})
				.toArray()
				.subscribe(response => {
					expect(response[0].timestamp < response[1].timestamp).to.be.true;
					expect(response[1].timestamp < response[2].timestamp).to.be.true;

					expect(response).to.deep.equal([{
						direction: 'OUT',
						distance: 1000,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '3',
						timestamp: response[0].timestamp,
						type: 'byTimestamp'
					}, {
						direction: 'OUT',
						distance: 1,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '4',
						timestamp: response[1].timestamp,
						type: 'byTimestamp'
					}, {
						direction: 'OUT',
						distance: 1,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '2',
						timestamp: response[2].timestamp,
						type: 'byTimestamp'
					}]);
				}, null, done);
		});

		it('should get edges desc', done => {
			store.getEdgesByTimestamp({
					namespace: app.namespace,
					entity: 'entity-2',
					fromNode: '1',
					direction: 'OUT',
					desc: true
				})
				.toArray()
				.subscribe(response => {
					expect(response[0].timestamp > response[1].timestamp).to.be.true;
					expect(response[1].timestamp > response[2].timestamp).to.be.true;

					expect(response).to.deep.equal([{
						direction: 'OUT',
						distance: 1,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '2',
						timestamp: response[0].timestamp,
						type: 'byTimestamp'
					}, {
						direction: 'OUT',
						distance: 1,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '4',
						timestamp: response[1].timestamp,
						type: 'byTimestamp'
					}, {
						direction: 'OUT',
						distance: 1000,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '3',
						timestamp: response[2].timestamp,
						type: 'byTimestamp'
					}]);
				}, null, done);
		});

		it('should get by timestamp', done => {
			store.getEdgesByTimestamp({
					namespace: app.namespace,
					entity: 'entity-2',
					fromNode: '1',
					timestamp: [now - 10, now - 5],
					direction: 'OUT'
				})
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([{
						direction: 'OUT',
						distance: 1000,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '3',
						timestamp: response[0].timestamp,
						type: 'byTimestamp'
					}, {
						direction: 'OUT',
						distance: 1,
						entity: 'entity-2',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '4',
						timestamp: response[1].timestamp,
						type: 'byTimestamp'
					}]);
				}, null, done);
		});

		it('should get by limit', done => {
			store.getEdgesByTimestamp({
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

	describe('setEdgeByDistance', () => {
		it('should throw if invalid', () => {
			expect(() => store.setEdgeByDistance()).to.throw('distance, entity, fromNode, namespace, toNode are missing or wrong.');
		});

		it('should return inserted edges', done => {
			store.setEdgeByDistance({
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
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: null,
						distance: 1,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0'
					}]);
				}, null, done);
		});

		it('should return inserted edges with direction', done => {
			store.setEdgeByDistance({
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
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: 'IN',
						distance: 1,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0'
					}]);
				}, null, done);
		});
	});

	describe('setEdgeByTimestamp', () => {
		it('should throw if invalid', () => {
			expect(() => store.setEdgeByTimestamp()).to.throw('entity, fromNode, namespace, timestamp, toNode are missing or wrong.');
		});

		it('should return inserted edges', done => {
			store.setEdgeByTimestamp({
					namespace: app.namespace,
					entity: 'entity',
					fromNode: '0',
					toNode: '1',
					timestamp: 1
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						direction: null,
						entity: 'entity',
						fromNode: '0',
						namespace: 'graph-1',
						toNode: '1',
						timestamp: 1
					}, {
						direction: null,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0',
						timestamp: 1
					}]);
				}, null, done);
		});

		it('should return inserted edges with direction', done => {
			store.setEdgeByTimestamp({
					namespace: app.namespace,
					entity: 'entity',
					fromNode: '0',
					toNode: '1',
					direction: 'OUT',
					timestamp: 1
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						direction: 'OUT',
						timestamp: 1,
						entity: 'entity',
						fromNode: '0',
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: 'IN',
						timestamp: 1,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0'
					}]);
				}, null, done);
		});
	});

	describe('incrementEdgeByDistance', () => {
		it('should throw if invalid', () => {
			expect(() => store.incrementEdgeByDistance()).to.throw('distance, entity, fromNode, namespace, toNode are missing or wrong.');
		});

		it('should return inserted edges', done => {
			store.incrementEdgeByDistance({
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
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: null,
						distance: 0.9,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0'
					}]);
				}, null, done);
		});

		it('should return inserted edges without increment', done => {
			store.incrementEdgeByDistance({
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
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: null,
						distance: 1,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0'
					}]);
				}, null, done);
		});

		it('should return inserted edges with direction', done => {
			store.incrementEdgeByDistance({
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
						namespace: 'graph-1',
						toNode: '1'
					}, {
						direction: 'IN',
						distance: 0.9,
						entity: 'entity',
						fromNode: '1',
						namespace: 'graph-1',
						toNode: '0'
					}]);
				}, null, done);
		});
	});

	describe('mergeEdge', () => {
		it('should throw if invalid', () => {
			expect(() => store.mergeEdge()).to.throw('namespace, fromNode, newNode are missing or wrong.');
		});

		it('should not change if newNode is same', done => {
			Observable.forkJoin(
					store.getEdges({
						namespace: app.namespace,
						fromNode: '1'
					})
					.toArray(),
					store.mergeEdge({
						namespace: app.namespace,
						fromNode: '1',
						newNode: '1'
					})
					.toArray()
				)
				.subscribe(response => {
					expect(response[1].length).to.equal(response[0].length);
					expect(response[1]).to.deep.include(response[0][0]);
					expect(response[1]).to.deep.include(response[0][1]);
					expect(response[1]).to.deep.include(response[0][2]);
					expect(response[1]).to.deep.include(response[0][3]);
					expect(response[1]).to.deep.include(response[0][4]);
					expect(response[1]).to.deep.include(response[0][5]);
					expect(response[1]).to.deep.include(response[0][6]);
					expect(response[1]).to.deep.include(response[0][7]);
				}, null, done);
		});

		it('should update fromNode', done => {
			Observable.forkJoin(
					store.getEdges({
						namespace: app.namespace,
						fromNode: '1'
					})
					.toArray(),
					store.mergeEdge({
						namespace: app.namespace,
						fromNode: '1',
						newNode: '2'
					})
					.toArray()
				)
				.subscribe(response => {
					expect(response[1].length).to.equal(response[0].length);
					expect(response[1]).to.deep.include(_.extend(response[0][0], {
						fromNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][1], {
						toNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][2], {
						fromNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][3], {
						toNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][4], {
						fromNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][5], {
						toNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][6], {
						fromNode: '2'
					}));
					expect(response[1]).to.deep.include(_.extend(response[0][7], {
						toNode: '2'
					}));
				}, null, done);
		});

		it('should return empty if inexistent', done => {
			store.mergeEdge({
					namespace: app.namespace,
					fromNode: 'inexistent',
					newNode: '1'
				})
				.toArray()
				.subscribe(response => {
					expect(response).to.deep.equal([]);
				}, null, done);
		});

		it('should increment distances proportionally and set last timestamp', done => {
			Observable.forkJoin(
					store.setEdgeByDistance({
						distance: 0.9,
						entity: 'entity',
						namespace: app.namespace,
						fromNode: '1',
						toNode: '2',
					}),
					store.setEdgeByTimestamp({
						entity: 'entity',
						namespace: app.namespace,
						fromNode: '1',
						toNode: '2',
						timestamp: 20
					}),
					store.setEdgeByDistance({
						distance: 0.9,
						entity: 'entity',
						namespace: app.namespace,
						fromNode: '3',
						toNode: '2',
					}),
					store.setEdgeByTimestamp({
						entity: 'entity',
						namespace: app.namespace,
						fromNode: '3',
						toNode: '2',
						timestamp: 10
					})
				)
				.mergeMap(() => store.mergeEdge({
					namespace: app.namespace,
					fromNode: '3',
					newNode: '1'
				}))
				.toArray()
				.subscribe(response => {
					const byDistance = _.filter(response, {
						type: 'byDistance'
					});

					expect(byDistance[0].distance).to.equal(0.8);
					expect(byDistance[1].distance).to.equal(0.8);

					const byTimestamp = _.filter(response, {
						type: 'byTimestamp'
					});

					expect(byTimestamp[0].timestamp).to.equal(20);
					expect(byTimestamp[1].timestamp).to.equal(20);
				}, null, done);
		});
	});

	describe('setNode', () => {
		it('should throw if invalid', () => {
			expect(() => store.setNode()).to.throw('data, id, namespace are missing or wrong.');
		});

		it('should return settled', done => {
			store.setNode({
					data: {
						a: 1
					},
					id: '0',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						data: {
							a: 1
						},
						namespace: 'graph-1',
						id: '0'
					});
				}, null, done);
		});
	});

	describe('setNodes', () => {
		it('should throw if invalid', () => {
			expect(() => store.setNodes()).to.throw('namespace, values are missing or wrong.');
		});

		it('should return settled', done => {
			store.setNodes({
					namespace: app.namespace,
					values: [{
						data: {
							a: 1
						},
						id: '0',
					}, {
						data: {
							a: 1
						},
						id: '1',
					}]
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						data: {
							a: 1
						},
						id: '0',
						namespace: 'graph-1'
					}, {
						data: {
							a: 1
						},
						id: '1',
						namespace: 'graph-1'
					}]);
				}, null, done);
		});

		it('should return settled (without array)', done => {
			store.setNodes({
					namespace: app.namespace,
					values: {
						data: {
							a: 1
						},
						id: '0',
					}
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						data: {
							a: 1
						},
						id: '0',
						namespace: 'graph-1'
					}]);
				}, null, done);
		});
	});

	describe('getNode', () => {
		it('should throw if invalid', () => {
			expect(() => store.getNode()).to.throw('id, namespace are missing or wrong.');
		});

		it('should return', done => {
			store.getNode({
					id: '0',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						data: {
							a: 1
						},
						namespace: 'graph-1',
						id: '0'
					});
				}, null, done);
		});

		it('should null if inexistent', done => {
			store.getNode({
					id: '3',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.be.null;
				}, null, done);
		});
	});

	describe('getNodes', () => {
		it('should throw if invalid', () => {
			expect(() => store.getNodes()).to.throw('ids, namespace are missing or wrong.');
		});

		it('should return', done => {
			store.getNodes({
					ids: ['0', '2', '1'],
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						data: {
							a: 1
						},
						id: '0',
						namespace: 'graph-1'
					}, null, {
						data: {
							a: 1
						},
						id: '1',
						namespace: 'graph-1'
					}]);
				}, null, done);
		});

		it('should return (without array)', done => {
			store.getNodes({
					ids: '0',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal([{
						data: {
							a: 1
						},
						id: '0',
						namespace: 'graph-1'
					}]);
				}, null, done);
		});

	});

	describe('updateNode', () => {
		it('should throw if invalid', () => {
			expect(() => store.updateNode()).to.throw('data, id, namespace are missing or wrong.');
		});

		it('should return settled', done => {
			store.updateNode({
					data: {
						a: 2
					},
					id: '0',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						data: {
							a: 2
						},
						namespace: 'graph-1',
						id: '0'
					});
				}, null, done);
		});

		it('should return settled if inexistent', done => {
			store.updateNode({
					data: {
						a: 2
					},
					id: '2',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						data: {
							a: 2
						},
						namespace: 'graph-1',
						id: '2'
					});
				}, null, done);
		});
	});

	describe('deleteNode', () => {
		it('should throw if invalid', () => {
			expect(() => store.deleteNode()).to.throw('id, namespace are missing or wrong.');
		});

		it('should return settled', done => {
			store.deleteNode({
					id: '0',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.deep.equal({
						namespace: 'graph-1',
						id: '0'
					});
				}, null, done);
		});

		it('should return null if inexistent', done => {
			store.deleteNode({
					id: '2',
					namespace: app.namespace
				})
				.subscribe(response => {
					expect(response).to.be.null;
				}, null, done);
		});
	});
});
