const joi = require('joi');

const direction = joi.valid(
        null,
        'IN',
        'OUT'
    )
    .default(null);

module.exports = {
    direction
};