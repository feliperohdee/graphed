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
			redis: app.redis
		});
	});

	beforeEach(() => {
		node = new Node({
			namespace: app.namespace,
			redis: app.redis
		});
	});

	describe('constructor', () => {
		it('should throw if no namespace', () => {
			expect(() => new Node({
				redis: app.redis
			})).to.throw('namespace is missing.');
		});

		it('should throw if no redis', () => {
			expect(() => new Node({
				namespace: app.namespace
			})).to.throw('redis is missing.');
		});

		it('should have namespace', () => {
			expect(node.namespace).to.be.a('string');
		});

		it('should have redis', () => {
			expect(node.redis).to.be.an('object');
		});
	});

	describe('delete', () => {
		beforeEach(done => {
			Observable.forkJoin(
					node.set({
						node: '1',
						data: {}
					}),
					node.set({
						node: '2',
						data: {}
					})
				)
				.subscribe(null, null, done);
		});

		after(done => {
			Observable.forkJoin(
					node.delete({
						node: '1'
					}),
					node.delete({
						node: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no node', done => {
			node.delete()
				.subscribe(null, err => {
					expect(err.message).to.equal('node is missing.');
					done();
				});
		});

		it('should delete', done => {
			Observable.forkJoin(
					node.delete({
						node: '1'
					}),
					node.delete({
						node: '2'
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						node: '1'
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						node: '2'
					});
				}, null, done);
		});

		it('should not delete', done => {
			node.delete({
					node: '3'
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
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}),
					node.set({
						node: '2',
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
						node: '1'
					}),
					node.delete({
						node: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no node', done => {
			node.get()
				.subscribe(null, err => {
					expect(err.message).to.equal('node is missing.');
					done();
				});
		});

		it('should get', done => {
			Observable.forkJoin(
					node.get({
						node: '1'
					}),
					node.get({
						node: '2'
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						node: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});

		it('should not get', done => {
			node.get({
					node: '3'
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
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}),
					node.set({
						node: '2',
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
						node: '1'
					}),
					node.delete({
						node: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no nodes', done => {
			node.multiGet()
				.subscribe(null, err => {
					expect(err.message).to.equal('nodes are missing.');
					done();
				});
		});

		it('should throw if wrong nodes', done => {
			node.multiGet({
					nodes: true
				})
				.subscribe(null, err => {
					expect(err.message).to.equal('nodes are missing.');
					done();
				});
		});

		it('should multi get', done => {
			node.multiGet({
					nodes: ['1', '2', '3']
				})
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						node: '2',
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
					node: '3'
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
						node: '1'
					}),
					node.delete({
						node: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no node', done => {
			node.set()
				.subscribe(null, err => {
					expect(err.message).to.equal('node is missing.');
					done();
				});
		});

		it('should set', done => {
			Observable.forkJoin(
					node.set({
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}),
					node.set({
						node: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						node: '2',
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
						node: '1'
					}),
					node.delete({
						node: '2'
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
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					}, {
						node: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					}]
				})
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						node: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});
	});

	describe('patch', () => {
		beforeEach(done => {
			node.set({
					node: '1',
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
						node: '1'
					}),
					node.delete({
						node: '2'
					})
				)
				.subscribe(null, null, done);
		});

		it('should throw if no node', done => {
			node.patch()
				.subscribe(null, err => {
					expect(err.message).to.equal('node is missing.');
					done();
				});
		});

		it('should patch', done => {
			Observable.forkJoin(
					node.patch({
						node: '1',
						data: {
							data111: 'data111'
						}
					}),
					node.patch({
						node: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					})
				)
				.subscribe(response => {
					expect(response[0]).to.deep.equal({
						namespace: 'graph-1',
						node: '1',
						data: {
							data1: 'data1',
							data11: 'data11',
							data111: 'data111'
						}
					});

					expect(response[1]).to.deep.equal({
						namespace: 'graph-1',
						node: '2',
						data: {
							data2: 'data2',
							data22: 'data22'
						}
					});
				}, null, done);
		});
	});
});
