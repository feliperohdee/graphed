const AWS = require('aws-sdk');
const {
    DynamoDB
} = require('smallorange-dynamodb-client');
const {
    DynamoDBStore
} = require('../');

class App {
    constructor() {
        this.namespace = 'spec';
        this.dynamoDb = new DynamoDB({
            client: new AWS.DynamoDB({
                endpoint: 'http://localhost:9090',
                region: 'us-east-1'
            })
        });

        this.store = new DynamoDBStore({
            dynamoDb: this.dynamoDb,
            tableName: 'graph'
        });
    }
}

module.exports = new App();