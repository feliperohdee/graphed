const AWS = require('aws-sdk');
const {
    DynamoDB
} = require('rxjs-dynamodb-client');
const {
    DynamoStore
} = require('../models');

class App {
    constructor() {
        this.dynamodb = new DynamoDB({
            client: new AWS.DynamoDB({
                endpoint: 'http://localhost:9090',
                region: 'us-east-1'
            })
        });

        this.store = new DynamoStore({
            dynamodb: this.dynamodb,
            tableName: 'specGraph',
            ttl: 365 * 24 * 60 * 60 * 1000 // 1 year
        });
    }
}

module.exports = new App();