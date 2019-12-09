const rx = require('rxjs');
const rxop = require('rxjs/operators');

const parseFirehoseRecordsData = records => {
    return rx.from(records || [])
        .pipe(
            rxop.map(record => {
                try {
                    return JSON.parse(Buffer.from(record.data, 'base64'));
                } catch (err) {
                    return null;
                }
            }),
            rxop.filter(Boolean)
        );
};

const toBase64 = data => {
    return Buffer.from(JSON.stringify(data))
        .toString('base64');
};

module.exports = {
    parseFirehoseRecordsData,
    toBase64
};