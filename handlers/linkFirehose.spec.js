const _ = require('lodash');
const chai = require('chai');
const rx = require('rxjs');
const rxop = require('rxjs/operators');

const linkFirehose = require('./linkFirehose');
const testing = require('../testing');
const {
    Graph
} = require('../models');
const {
    toBase64
} = require('./util');

const expect = chai.expect;
const namespace = 'spec';
const graph = new Graph({
    store: testing.app.store
}, {
    firehose: {
        concurrency: 1,
        stream: 'stream'
    }
});

const handler = linkFirehose(graph);

describe('handlers/linkFirehose.js', () => {
    after(done => {
        rx.forkJoin(
                graph.deleteByNode({
                    fromNode: '0',
                    namespace
                })
                .pipe(
                    rxop.toArray()
                )
            )
            .subscribe(null, null, done);
    });

    it('should throw if no firehose configured', () => {
        expect(() => linkFirehose(new Graph({
            store: testing.app.store
        }))).to.throw('NoFirehoseConfiguredError');
    });

    it('should process empty', done => {
        handler({}, {}, (err, data) => {
            expect(data).to.deep.equal({
                records: []
            });
            done();
        });
    });

    it('should process', done => {
        handler({
            records: [{
                recordId: 'id',
                data: toBase64({
                    entity: 'entity',
                    fromNode: '0',
                    namespace,
                    toNode: '1'
                })
            }, {
                recordId: 'id',
                data: toBase64({
                    entity: 'entity',
                    fromNode: '0',
                    namespace,
                    toNode: '1'
                })
            }, {
                recordId: 'id',
                data: toBase64({
                    entity: 'entity',
                    fromNode: '1',
                    namespace,
                    toNode: '0'
                })
            }]
        }, {}, (err, data) => {
            expect(data).to.deep.equal({
                records: _.times(3, i => ({
                    recordId: 'id',
                    result: 'Ok',
                    data: data.records[i].data
                }))
            });
            done();
        });
    });
});