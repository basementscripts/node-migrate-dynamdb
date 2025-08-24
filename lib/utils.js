const { BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb')
const { marshall } = require('@aws-sdk/util-dynamodb')
const lodash = require('lodash')
const db = require('./dynamodb')

const buildBulkPutInput = (params) => ({
	PutRequest: {
		Item: Object.entries(params).reduce((acc, [k, v]) => ({ ...acc, [k]: marshall(v) }), {})
	}
})

module.exports.buildBulkPutInput = buildBulkPutInput

const groupChunk = (chunk) => {
	return chunk.reduce((acc, { table, data }) => {
		if (acc[table]) {
			acc[table].push(buildBulkPutInput(data))
		} else {
			acc[table] = [buildBulkPutInput(data)]
		}
		return acc
	}, {})
}

module.exports.groupChunk = groupChunk

const batchWriteDb = async (records, chunkSize = 25) => {
	const chunks = lodash.chunk(records, chunkSize)
	while (chunks.length > 0) {
		const chunk = chunks.shift()

		const cmd = new BatchWriteItemCommand({
			RequestItems: groupChunk(chunk)
		})
		const results = await db.send(cmd)
		console.log(JSON.stringify(results, null, 2))
	}
}

module.exports.batchWriteDb = batchWriteDb
