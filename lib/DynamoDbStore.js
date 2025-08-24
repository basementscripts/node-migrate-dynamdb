const { pick } = require('lodash')
const db = require('./dynamodb')

const TABLE_NAME = process.env.DATA_MIGRATIONS_TABLE_NAME || 'datamigrations'

/**
 * Serialize migrations to a format that can be stored in the database
 * @param {Object[] } migrations
 * @returns {Object[]}
 */
const serializeMigrations = (migrations) => {
	return migrations.map((migration) => {
		return pick(migration, ['title', 'description', 'timestamp'])
	})
}

/**
 * List migrations from the database
 * @param {Object} db
 * @returns {Promise<Object[]>}
 */
const listMigrations = async () => {
	const data = await db.list(TABLE_NAME)
	if (data.items.length === 0) {
		console.log(
			'Cannot read migrations from database. If this is the first time you run migrations, then this is normal.'
		)
		return []
	}
	return data.items
}

/**
 * Get the last run migration
 * @param {Object[]} data
 * @returns {number}
 */
const getLastRun = (data) => {
	return data.sort((a, b) => b.timestamp - a.timestamp)[0]?.timestamp
}

/**
 * Get migrations to update
 * @param {Object[]} existingMigrations
 * @param {Object[]} migrations
 * @returns {Object[]}
 */
const getMigrationsToUpdate = (existingMigrations, migrations) => {
	return existingMigrations
		.filter((migration) =>
			migrations.find((m) => m.title === migration.title && m.timestamp !== migration.timestamp)
		)
		.map((migration) => ({
			...migration,
			timestamp: migrations.find((m) => m.title === migration.title)?.timestamp
		}))
}

/**
 * Get migrations to create
 * @param {Object[]} existingMigrations
 * @param {Object[]} migrations
 * @returns {Object[]}
 */
const getMigrationsToCreate = (existingMigrations, migrations) => {
	return migrations.filter(
		(migration) => !existingMigrations.find((m) => m.title === migration.title)
	)
}

/**
 * Create a migration
 * @param {Object} migration
 * @returns {Promise<Object>}
 */
const createMigration = async (migration) => {
	// extract the timestamp from the migration title
	const createdAt = migration.title.match(/^\d+/)?.[0]
	return db.create(TABLE_NAME, {
		...migration,
		createdAt: +createdAt
	})
}

/**
 * Update a migration
 * @param {Object} migration
 * @returns {Promise<Object>}
 */
const updateMigration = async (migration) => {
	return db.update(TABLE_NAME, migration)
}

/**
 * DynamoDbStore
 * @class
 */
class DynamoDbStore {
	/**
	 * Logger
	 * @type {Console}
	 */
	static logger = console

	/**
	 * Load migrations
	 * @param {Function} fn
	 * @param {Object} db
	 * @returns {Promise<Object>}
	 */
	async load(fn) {
		try {
			// get existing migrations
			const dataMigrations = await listMigrations(db)
			// sort by timestamp, ensure the last run is the last one
			const data = dataMigrations.sort((a, b) => a.timestamp - b.timestamp)

			return fn(null, {
				migrations: data,
				lastRun: getLastRun(data)
			})
		} catch (e) {
			throw e
		}
	}

	/**
	 * Save migrations
	 * @param {Object} set
	 * @param {Function} fn
	 * @param {Object} db
	 * @returns {Promise<Object>}
	 */
	async save(set, fn) {
		try {
			// get existing migrations
			const dataMigrations = await listMigrations(db)
			// sort by timestamp, ensure the last run is the last one
			const data = dataMigrations.sort((a, b) => a.timestamp - b.timestamp)
			// serialize migrations
			const migrations = serializeMigrations(set.migrations)
			// filter existing migrations
			const existingMigrations = data.filter((migration) =>
				migrations.find((m) => m.title === migration.title)
			)
			// filter migrations to update
			const migrationsToUpdate = getMigrationsToUpdate(existingMigrations, migrations)
			// filter migrations to create
			const migrationsToCreate = getMigrationsToCreate(existingMigrations, migrations)
			// promises to run
			const promises = []

			// create migrations
			if (migrationsToCreate.length > 0) {
				promises.push(...migrationsToCreate.map(createMigration))
			}

			// update migrations
			if (migrationsToUpdate.length > 0) {
				promises.push(...migrationsToUpdate.map(updateMigration))
			}

			// run promises
			const results = await Promise.all(promises)

			// merge migrations
			const mergedMigrations = [
				...existingMigrations.filter((m) => !migrationsToUpdate.find((m2) => m2.id === m.id)),
				...results
			]

			return fn(null, {
				lastRun: set?.lastRun,
				migrations: mergedMigrations
			})
		} catch (e) {
			throw e
		}
	}
}

module.exports = DynamoDbStore
