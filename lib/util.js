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

const validate = (args = {}, required = {
	namespace: true
}) => {
	const {
		entity,
		fromNode,
		toNode,
		namespace,
	} = args;

	const errors = _.reduce(required, (reduction, value, key) => {
		if (value === true && _.isUndefined(args[key])) {
			return reduction.concat(key);
		} else if (typeof value === 'string' && typeof args[key] !== value) {
			return reduction.concat(key);
		}

		return reduction;
	}, []);

	if (errors.length) {
		throw new Error(`${errors.join(', ')} ${errors.length === 1 ? 'is' : 'are'} missing or wrong.`);
	}

	return args;
};

const validateStore = (store, requiredKeys = []) => {
	const missingKeys = _.reduce(requiredKeys, (reduction, key) => {
		if (!store[key]) {
			return reduction.concat(key);
		}

		return reduction;
	}, []);

	if (missingKeys.length) {
		throw new Error(`Invalid store, missing ${missingKeys.join(', ')}`);
	}

	return store;
};

module.exports = {
	invertDirection,
	pickEdgeData,
	validate,
	validateStore
};
