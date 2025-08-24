const { DynamoDBClient } = require('@aws-sdk/client-dynamodb')

const config = {
	// default to us-east-1
	region: process.env.AWS_REGION || 'us-east-1'
}
// support local dynamodb
if (process.env.AWS_ENDPOINT) {
	config.endpoint = process.env.AWS_ENDPOINT
}

const client = new DynamoDBClient(config)
module.exports = DynamoDBDocumentClient.from(client, {
	marshallOptions: { removeUndefinedValues: true }
})
