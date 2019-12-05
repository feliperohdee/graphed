const joi = require('@hapi/joi');

const direction = joi.valid(
        null,
        'IN',
        'OUT'
    )
    .default(null);

module.exports = {
    direction
};