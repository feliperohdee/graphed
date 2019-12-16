const rx = require('rxjs');

const dynamoDbStore = require('./dynamoDbStore');
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
    dynamoDbStore,
    graph,
    validate
};