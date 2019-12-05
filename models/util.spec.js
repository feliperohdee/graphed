const chai = require('chai');

const {
	invertDirection,
	pickEdgeData
} = require('../models/util');

const expect = chai.expect;

describe('models/util.js', () => {
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
				toNode: 'toNode'
			})).to.deep.equal({
				direction: 'direction',
				distance: 'distance',
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: 'namespace',
				toNode: 'toNode'
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
				toNode: 'toNode'
			}, {
				namespace: 'namespace 2',
				toNode: 'toNode 2'
			})).to.deep.equal({
				direction: 'direction',
				distance: 'distance',
				entity: 'entity',
				fromNode: 'fromNode',
				namespace: 'namespace 2',
				toNode: 'toNode 2'
			});
		});
	});
});
