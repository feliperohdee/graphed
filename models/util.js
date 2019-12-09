const _ = require('lodash');

const invertDirection = direction => {
	if (!direction) {
		return null;
	}

	return direction === 'IN' ? 'OUT' : 'IN';
};

const pickEdgeData = (...args) => {
	args = _.extend({
		direction: null
	}, ...args);

	return _.pick(args, [
		'direction',
		'distance',
		'entity',
		'fromNode',
		'namespace',
		'toNode',
		'type'
	]);
};

module.exports = {
	invertDirection,
	pickEdgeData
};
