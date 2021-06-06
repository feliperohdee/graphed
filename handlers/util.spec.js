const chai = require('chai');
const rxop = require('rxjs/operators');

const testing = require('../testing');
const {
    parseFirehoseRecordsData,
    toBase64
} = require('./util');

const expect = chai.expect;

describe('handlers/util.js', () => {
    describe('parseFirehoseRecordsData', () => {
        it('should process empty', done => {
            parseFirehoseRecordsData()
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([]);
                }, null, done));
        });

        it('should process and filter invalid', done => {
            parseFirehoseRecordsData([{
                    recordId: 'id',
                    data: toBase64({
                        record: 'record'
                    })
                }, {
                    recordId: 'id',
                    data: {
                        record: 'record'
                    }
                }, {
                    recordId: 'id',
                    data: toBase64({
                        record: 'record'
                    })
                }, {
                    recordId: 'id',
                    data: 'invalid' + toBase64({
                        record: 'record'
                    })
                }])
                .pipe(
                    rxop.toArray()
                )
                .subscribe(testing.rx(response => {
                    expect(response).to.deep.equal([{
                        record: 'record'
                    }, {
                        record: 'record'
                    }]);
                }, null, done));
        });
    });

    describe('toBase64', () => {
        it('should b64', () => {
            expect(JSON.parse(Buffer.from(toBase64({
                a: 'a'
            }), 'base64'))).to.deep.equal({
                a: 'a'
            });
        });
    });
});