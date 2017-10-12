const _ = require('lodash');
const {
	Observable
} = require('rxjs');

const {
	validate,
	validateStore
} = require('./util');

const requiredStoreKeys = [
	'deleteNode',
	'getNode',
	'getNodes',
	'setNode',
	'setNodes',
	'updateNode'
];

module.exports = class Node {
	constructor(args = {}) {
		args = validate(args, {
			namespace: true,
			store: true
		});

		this.namespace = args.namespace
		this.store = validateStore(args.store, requiredStoreKeys);
	}

	delete(args = {}) {
		if (!args.id) {
			return Observable.throw(new Error('id is missing.'));
		}

		return this.store.deleteNode(_.extend({}, args, {
			namespace: this.namespace
		}));
	}

	get(args = {}) {
		if (!args.id) {
			return Observable.throw(new Error('id is missing.'));
		}

		return this.store.getNode(_.extend({}, args, {
			namespace: this.namespace
		}));
	}

	multiGet(args = {}) {
		if (!args.ids) {
			return Observable.throw(new Error('ids are missing.'));
		}

		return this.store.getNodes(_.extend({}, args, {
			namespace: this.namespace
		}));
	}

	set(args = {}) {
		if (!args.id) {
			return Observable.throw(new Error('id is missing.'));
		}

		if (!args.data) {
			return Observable.throw(new Error('data is missing.'));
		}

		return this.store.setNode(_.extend({}, args, {
			namespace: this.namespace
		}));
	}

	multiSet(args = {}) {
		if (!args.values) {
			return Observable.throw(new Error('values are missing.'));
		}

		return this.store.setNodes(_.extend({}, args, {
			namespace: this.namespace
		}));
	}

	update(args = {}) {
		if (!args.id) {
			return Observable.throw(new Error('id is missing.'));
		}

		if (!args.data) {
			return Observable.throw(new Error('data is missing.'));
		}

		return this.store.updateNode(_.extend({}, args, {
			namespace: this.namespace
		}));
	}
}
