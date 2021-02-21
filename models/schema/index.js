const rx = require('rxjs');

const dynamoStore = require('./dynamoStore');
const graph = require('./graph');
const validate = (schema, value) => {
    return rx.from(schema.validateAsync(value, {
        abortEarly: false,
        stripUnknown: {
            arrays: false,
            objects: true
        }
    }));
};

module.exports = {
    dynamoStore,
    graph,
    validate
};