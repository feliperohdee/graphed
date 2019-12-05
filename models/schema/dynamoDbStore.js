const joi = require('@hapi/joi');

const common = require('./common');

const constructor = joi.object({
    dynamoDb: joi.object()
        .unknown()
        .required(),
    tableName: joi.string()
        .required()
});

const countEdges = joi.object({
    direction: common.direction,
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    namespace: joi.string()
        .required()
});

const deleteEdge = joi.object({
    direction: common.direction,
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    inverse: joi.boolean(),
    namespace: joi.string()
        .required(),
    toNode: joi.string()
        .required()
});

const deleteEdges = joi.object({
    direction: common.direction,
    entity: joi.string(),
    fromNode: joi.string(),
    namespace: joi.string()
        .required()
});

const getEdges = joi.object({
    direction: common.direction,
    entity: joi.string(),
    fromNode: joi.string(),
    inverse: joi.boolean()
        .default(true),
    namespace: joi.string()
        .required()
});

const getEdgesByDistance = joi.object({
    desc: joi.boolean(),
    direction: common.direction,
    distance: joi.array()
        .items(joi.number())
        .min(1)
        .max(2),
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    inverse: joi.boolean()
        .default(true),
    limit: joi.number(),
    namespace: joi.string()
        .required()
});

const setEdge = joi.object({
    direction: common.direction,
    distance: joi.number()
        .required(),
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    namespace: joi.string()
        .required(),
    toNode: joi.string()
        .required()
});

module.exports = {
    constructor,
    countEdges,
    deleteEdge,
    deleteEdges,
    getEdges,
    getEdgesByDistance,
    setEdge
};