const _ = require('lodash');
const error = require('simple-beauty-error');
const rxop = require('rxjs/operators');

const {
    Edge
} = require('../models');
const {
    parseFirehoseRecordsData
} = require('./util');

module.exports = (argsOrEdge, options) => {
    const edge = argsOrEdge instanceof Edge ? argsOrEdge : new Edge(argsOrEdge, options);

    if(!edge.options.firehose) {
        throw new Error('no firehose configured.');
    }

    return (event, context, callback) => {
        edge.processFirehose(
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