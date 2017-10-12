const _ = require('lodash');
const chai = require('chai');
const {
	invertDirection,
	pickEdgeData,
	validate
} = require('../lib/util');

const expect = chai.expect;

describe('util.js', () => {
	describe('invertDirection', () => {
		it('should invert directions', () => {
			expect(invertDirection('OUT')).to.equal('IN');
			expect(invertDirection('IN')).to.equal('OUT');
			expect(invertDirection(null)).to.be.null;
		});
	});

	describe('pickEdgeData', () => {
		it('should pick', () => {
			expect(pickEdgeData({
				direction: 'direction',
				distance: 'distance',
				entity: 'entity',
				forbidden: 'forbidden',
				fromNode: 'fromNode',
				namespace: 'namespace',
				toNode: 'toNode',
				timestamp: 'timestamp',
				type: 'type'
			})).to.deep.equal({
				direction: 'direction',
				distance: 'distance',
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: 'namespace',
				toNode: 'toNode',
				timestamp: 'timestamp',
				type: 'type'
			});
		});

		it('should pick with last precedence', () => {
			expect(pickEdgeData({
				direction: 'direction',
				distance: 'distance',
				entity: 'entity',
				forbidden: 'forbidden',
				fromNode: 'fromNode',
				namespace: 'namespace',
				toNode: 'toNode',
				timestamp: 'timestamp',
				type: 'type'
			}, {
				namespace: 'namespace 2',
				toNode: 'toNode 2',
				timestamp: 'timestamp 2',
				type: 'type 2'
			})).to.deep.equal({
				direction: 'direction',
				distance: 'distance',
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: 'namespace 2',
				toNode: 'toNode 2',
				timestamp: 'timestamp 2',
				type: 'type 2'
			});
		});
	});

	describe('validate', () => {
		it('should throw if missing', () => {
			expect(() => validate({}, {
				a: true
			})).to.throw('a is missing or wrong.');
		});

		it('should throw if many missing', () => {
			expect(() => validate({}, {
				a: true,
				b: true
			})).to.throw('a, b are missing or wrong.');
		});

		it('should throw if typeof mismatch', () => {
			expect(() => validate({}, {
				a: 'string',
				b: 'number'
			})).to.throw('a, b are missing or wrong.');
		});

		it('should return args if valid', () => {
			expect(validate({
				a: 'string',
				b: 4
			}, {
				a: 'string',
				b: 'number'
			})).to.deep.equal({
				a: 'string',
				b: 4
			});
		});
	});
});
