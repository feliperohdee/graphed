const joi = require('@hapi/joi');

const common = require('./common');

const allAll = joi.object({
    direction: common.direction,
    distance: joi.alternatives()
        .try(
            joi.function(),
            joi.number()
        ),
    entity: joi.string()
        .required(),
    namespace: joi.string(),
    value: joi.array()
        .items(joi.string())
        .min(1)
        .required()
});

const allByNode = joi.object({
    direction: common.direction,
    entity: joi.string(),
    fromNode: joi.string()
        .required(),
    inverse: joi.boolean()
        .default(true),
    namespace: joi.string(),
    onlyNodes: joi.boolean(),
    toNode: joi.string()
});

const closest = joi.object({
    direction: common.direction,
    distance: joi.array()
        .items(
            joi.number().min(0).max(1)
        )
        .min(1)
        .max(2),
    desc: joi.boolean(),
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    namespace: joi.string(),
    limit: joi.number()
});

const constructor = joi.object({
    decrementPath: joi.number()
        .default(1 / (10 ** 15)),
    defaultDirection: common.direction,
    defaultEntity: joi.string(),
    partition: joi.string()
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

const constructorOptions = joi.object({
    firehose: joi.object({
        concurrency: joi.number()
            .min(0)
            .max(1000)
            .default(100),
        stream: joi.string()
            .required()
    })
});

const count = joi.object({
    direction: common.direction,
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    namespace: joi.string()
});

const del = joi.object({
    direction: common.direction,
    entity: joi.string()
        .required(),
    fromNode: joi.string()
        .required(),
    inverse: joi.boolean(),
    namespace: joi.string(),
    toNode: joi.string()
        .required()
});

const delByNode = joi.object({
    direction: common.direction,
    entity: joi.string(),
    fromNode: joi.string()
        .required(),
    namespace: joi.string(),
    toNode: joi.string()
});

const link = joi.object({
    absoluteDistance: joi.number()
        .min(0)
        .max(1),
    direction: common.direction,
    distance: joi.number()
        .default(1),
    fromNode: joi.string()
        .required(),
    entity: joi.string()
        .required(),
    namespace: joi.string(),
    toNode: joi.string()
        .required()
});

const traverse = joi.object({
    concurrency: joi.number()
        .min(0)
        .max(50000)
        .default(50000),
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
    constructorOptions,
    count,
    del,
    delByNode,
    link,
    traverse
};