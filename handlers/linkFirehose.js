const _ = require('lodash');
const error = require('simple-beauty-error');
const rxop = require('rxjs/operators');

const {
    Graph
} = require('../models');
const {
    parseFirehoseRecordsData
} = require('./util');

module.exports = (argsOrGraph, options) => {
    const graph = argsOrGraph instanceof Graph ? argsOrGraph : new Graph(argsOrGraph, options);

    if(!graph.options.firehose) {
        throw new Error('NoFirehoseConfiguredError');
    }

    return (event, context, callback) => {
        graph.processFirehose(
                parseFirehoseRecordsData(event.records)
            )
            .pipe(
                rxop.toArray()
            )
            .subscribe(() => {
                callback(null, {
                    records: _.map(event.records, ({
                        data,
                        recordId
                    }) => ({
                        recordId,
                        result: 'Ok',
                        data
                    }))
                });
            }, err => {
                console.log(JSON.stringify(error(err, {}, 5), null, 2));
                callback(err);
            });
    };
};