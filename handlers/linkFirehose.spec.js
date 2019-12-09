const _ = require('lodash');
const chai = require('chai');
const rx = require('rxjs');
const rxop = require('rxjs/operators');

const app = require('../testing/dynamoDb');
const linkFirehose = require('./linkFirehose');
const {
    Edge
} = require('../models');
const {
    toBase64
} = require('./util');

const expect = chai.expect;
const edge = new Edge({
    partition: app.partition,
    store: app.store
}, {
    firehose: {
        concurrency: 1,
        stream: 'stream'
    }
});

const handler = linkFirehose(edge);

describe('handlers/linkFirehose.js', () => {
    after(done => {
        rx.forkJoin(
                edge.deleteByNode({
                    fromNode: '0'
                })
                .pipe(
                    rxop.toArray()
                )
            )
            .subscribe(null, null, done);
    });

    it('should throw if no firehose configured', () => {
        expect(() => linkFirehose(new Edge({
            partition: app.partition,
            store: app.store
        }))).to.throw('no firehose configured.');
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
                    toNode: '1'
                })
            }, {
                recordId: 'id',
                data: toBase64({
                    entity: 'entity',
                    fromNode: '0',
                    toNode: '1'
                })
            }, {
                recordId: 'id',
                data: toBase64({
                    entity: 'entity',
                    fromNode: '1',
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