const joi = require('@hapi/joi');

const common = require('./common');

const allAll = joi.object({
    collection: joi.array()
        .items(joi.string())
        .min(1)
        .required(),
    direction: common.direction,
    distance: joi.alternatives()
        .try(
            joi.function(),
            joi.number()
        ),
    entity: joi.string()
        .required(),
    prefix: joi.string()
        .allow('')
        .default('')
});

const allByNode = joi.object({
    direction: common.direction,
    entity: joi.string(),
    fromNode: joi.string()
        .required(),
    inverse: joi.boolean()
        .default(true),
    onlyNodes: joi.boolean(),
    prefix: joi.string()
        .allow('')
        .default(''),
    toNode: joi.string()
});

const closest = joi.object({
    direction: common.direction,
    desc: joi.boolean(),
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    prefix: joi.string()
        .allow('')
        .default('')
});

const constructor = joi.object({
    decrementPath: joi.number()
        .default(1 / (10 ** 15)),
    defaultDirection: common.direction,
    defaultEntity: joi.string(),
    namespace: joi.string()
        .required(),
    store: joi.object({
            countEdges: joi.function()
                .required(),
            deleteEdge: joi.function()
                .required(),
            deleteEdges: joi.function()
                .required(),
            getEdges: joi.function()
                .required(),
            getEdgesByDistance: joi.function()
                .required(),
            setEdge: joi.function()
                .required()
        })
        .unknown()
        .required()
});

const count = joi.object({
    direction: common.direction,
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    prefix: joi.string()
        .allow('')
        .default('')
});

const del = joi.object({
    direction: common.direction,
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    inverse: joi.boolean(),
    prefix: joi.string()
        .allow('')
        .default(''),
    toNode: joi.string()
        .required()
});

const delByNode = joi.object({
    direction: common.direction,
    entity: joi.string(),
    fromNode: joi.string()
        .required(),
    prefix: joi.string()
        .allow('')
        .default(''),
    toNode: joi.string()
});

const link = joi.object({
    absoluteDistance: joi.number()
        .min(0),
    direction: common.direction,
    distance: joi.number()
        .default(1),
    fromNode: joi.string()
        .required(),
    entity: joi.string()
        .required(),
    prefix: joi.string()
        .allow('')
        .default(''),
    toNode: joi.string()
        .required()
});

const traverse = joi.object({
    concurrency: joi.number()
        .default(Number.MAX_SAFE_INTEGER),
    jobs: joi.array()
        .items(closest.fork([
            'fromNode'
        ], schema => {
            return schema.optional();
        }))
        .required(),
    maxPath: joi.number()
        .default(30),
    minPath: joi.number()
        .default(2),
    remoteClosest: joi.function(),
    remoteClosestIndex: joi.number()
        .default(1)
});

module.exports = {
    allAll,
    allByNode,
    closest,
    constructor,
    count,
    del,
    delByNode,
    link,
    traverse
};