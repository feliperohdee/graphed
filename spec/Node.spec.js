const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const {
	Observable
} = require('rxjs');

const app = require('../testing');
const {
	Node
} = require('../');

chai.use(sinonChai);

const expect = chai.expect;

describe('Node.js', () => {
	let node;

	before(() => {
		node = new Node({
			namespace: app.namespace,
			store: app.store
		});
	});

	beforeEach(() => {
		node = new Node({
			namespace: app.namespace,
			store: app.store
		});
	});

	describe('constructor', () => {
		it('should throw if invalid', () => {
			expect(() => new Node()).to.throw('namespace, store are missing or wrong.');
		});

		it('should have namespace', () => {
			expect(node.namespace).to.be.a('string');
		});

		it('should have store', () => {
			expect(node.store).to.be.an('object');
		});

		it('should validate store', () => {
			expect(() => new Node({
				namespace: app.namespace,
				node,
				store: _.omit(app.store, ['deleteNode', 'getNode'])
			})).to.throw('Invalid store, missing deleteNode, getNode');
		});
	});

	describe('delete', () => {
		beforeEach(done => {
			Observable.forkJoin(
					node.set({
						data: {},
						id: '1'
					}),
					node.set({
						data: {},
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		after(done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no id', done => {
			node.delete()
				.subscribe(null, err => {
					expect(err.message).to.equal('id is missing.');
					done();
				});
		});

		it('should delete', done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						id: '1'
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						id: '2'
					});
				}, null, done);
		});

		it('should not delete', done => {
			node.delete({
					id: '3'
				})
				.subscribe(response => {
					expect(response).to.be.null;
				}, null, done);
		});
	});

	describe('get', () => {
		beforeEach(done => {
			Observable.forkJoin(
					node.set({
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}),
					node.set({
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					})
				)
				.subscribe(null, null, done);
		});

		after(done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no id', done => {
			node.get()
				.subscribe(null, err => {
					expect(err.message).to.equal('id is missing.');
					done();
				});
		});

		it('should get', done => {
			Observable.forkJoin(
					node.get({
						id: '1'
					}),
					node.get({
						id: '2'
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});

		it('should not get', done => {
			node.get({
					id: '3'
				})
				.subscribe(response => {
					expect(response).to.be.null;
				}, null, done);
		});
	});

	describe('multiGet', () => {
		beforeEach(done => {
			Observable.forkJoin(
					node.set({
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}),
					node.set({
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					})
				)
				.subscribe(null, null, done);
		});

		after(done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no ids', done => {
			node.multiGet()
				.subscribe(null, err => {
					expect(err.message).to.equal('ids are missing.');
					done();
				});
		});

		it('should throw if wrong nodes', done => {
			node.multiGet({
					nodes: true
				})
				.subscribe(null, err => {
					expect(err.message).to.equal('ids are missing.');
					done();
				});
		});

		it('should multi get', done => {
			node.multiGet({
					ids: ['1', '2', '3']
				})
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});

					expect(response[2]).to.be.null;
				}, null, done);
		});

		it('should not get', done => {
			node.get({
					id: '3'
				})
				.subscribe(response => {
					expect(response).to.be.null;
				}, null, done);
		});
	});

	describe('set', () => {
		after(done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no id', done => {
			node.set()
				.subscribe(null, err => {
					expect(err.message).to.equal('id is missing.');
					done();
				});
		});

		it('should set', done => {
			Observable.forkJoin(
					node.set({
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}),
					node.set({
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});
	});

	describe('multiSet', () => {
		after(done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no values', done => {
			node.multiSet()
				.subscribe(null, err => {
					expect(err.message).to.equal('values are missing.');
					done();
				});
		});

		it('should multi set', done => {
			node.multiSet({
					values: [{
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}, {
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					}]
				})
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});
	});

	describe('update', () => {
		beforeEach(done => {
			node.set({
					id: '1',
					data: {
						data1: 'data1',
						data11: 'data11'
					}
				})
				.subscribe(null, null, done);
		});

		after(done => {
			Observable.forkJoin(
					node.delete({
						id: '1'
					}),
					node.delete({
						id: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no id', done => {
			node.update()
				.subscribe(null, err => {
					expect(err.message).to.equal('id is missing.');
					done();
				});
		});

		it('should update', done => {
			Observable.forkJoin(
					node.update({
						id: '1',
						data: {
							data111: 'data111'
						}
					}),
					node.update({
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						id: '1',
						data: {
							data1: 'data1',
							data11: 'data11',
							data111: 'data111'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						id: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});
	});
});
