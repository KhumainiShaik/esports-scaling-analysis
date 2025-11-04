package com.agile.cloud.automation.coursework1;

@Grab(group='org.mongodb', module='mongodb-driver-sync', version='5.2.0')
import com.mongodb.client.*
import org.bson.Document
import java.util.Properties
import groovy.json.JsonOutput

// ---------- Load MongoDB credentials ----------
def props = new Properties()
new File("./resources/mongo.properties").withInputStream { props.load(it) }

def username = props['mongodb.username']
def password = props['mongodb.password']
def cluster = props['mongodb.cluster']
def databaseName = props['mongodb.database']
def histColl = props['mongodb.collections.historical']
def genColl = props['mongodb.collections.general']

// ---------- Build connection URI ----------
def uri = "mongodb+srv://${username}:${password}@${cluster}/?retryWrites=true&w=majority"

// ---------- Connect to MongoDB ----------
MongoClient mongoClient = MongoClients.create(uri)
def db = mongoClient.getDatabase(databaseName)
def historical = db.getCollection(histColl)
def general = db.getCollection(genColl)

println "\n=============================="
println " Exercise 4: Scalability Analysis"
println "=============================="

// =====================================================
// ⚙️ PAGINATION SETTINGS
// =====================================================
def pageSize = 5  // simulate batch query (smaller batch = better scalability)
def pageNumber = 1

def skip = (pageNumber - 1) * pageSize
println "\nPagination Enabled: Fetching Page ${pageNumber} (Page Size: ${pageSize})"

// =====================================================
// ⚙️ PERSISTENT FILE-BASED CACHE
// =====================================================
def cacheDir = new File("cache")
if (!cacheDir.exists()) {
    cacheDir.mkdir()
}

def cacheKey = "topGenresPage${pageNumber}"
def cacheFile = new File(cacheDir, "${cacheKey}.json")

// Check if cache exists and is less than 5 minutes old
if (cacheFile.exists() && (System.currentTimeMillis() - cacheFile.lastModified() < 300000)) {
    println "Cache Hit — Results loaded from file cache.\n"
    def cachedResults = new groovy.json.JsonSlurper().parse(cacheFile)
    cachedResults.each { doc ->
        println JsonOutput.prettyPrint(JsonOutput.toJson(doc))
        println "---------------------------"
    }
    mongoClient.close()
    return
}

println "Cache Miss — Running Aggregation Query...\n"

// =====================================================
// AGGREGATION PIPELINE
// =====================================================
def pipeline = [
	new Document('$match', new Document('Date', new Document('$gte', new Date(115, 0, 1)))
			.append('Earnings', new Document('$gt', 0))
			.append('Players', new Document('$gt', 0))
	),
	new Document('$addFields', [
		'Year': new Document('$year', '$Date'),
		'EarningsPerPlayer': new Document('$divide', ['$Earnings', '$Players'])
	]),
	new Document('$lookup', [
		from: genColl,
		localField: 'Game',
		foreignField: 'Game',
		as: 'gameDetails'
	]),
	new Document('$addFields', [
		'Genre': new Document('$arrayElemAt', ['$gameDetails.Genre', 0]),
		'PercentOffline': new Document('$arrayElemAt', ['$gameDetails.PercentOffline', 0])
	]),
	new Document('$match', new Document('Genre', new Document('$exists', true).append('$ne', null))),
	new Document('$group', [
		_id: [Genre: '$Genre', Year: '$Year', Game: '$Game'],
		TotalEarningsPerGame: new Document('$sum', '$Earnings'),
		TotalPlayersPerGame: new Document('$sum', '$Players'),
		TotalTournamentsPerGame: new Document('$sum', '$Tournaments'),
		AvgEarningsPerPlayer: new Document('$avg', '$EarningsPerPlayer'),
		AvgOfflinePercentage: new Document('$avg', '$PercentOffline')
	]),
	new Document('$sort', new Document('TotalEarningsPerGame', -1)),
	new Document('$group', [
		_id: [Genre: '$_id.Genre', Year: '$_id.Year'],
		TotalYearlyEarnings: new Document('$sum', '$TotalEarningsPerGame'),
		TotalPlayers: new Document('$sum', '$TotalPlayersPerGame'),
		TotalTournaments: new Document('$sum', '$TotalTournamentsPerGame'),
		GameCount: new Document('$sum', 1),
		AvgEarningsPerPlayer: new Document('$avg', '$AvgEarningsPerPlayer'),
		AvgOfflinePercentage: new Document('$avg', '$AvgOfflinePercentage'),
		TopGame: new Document('$first', '$_id.Game'),
		TopGameEarnings: new Document('$first', '$TotalEarningsPerGame')
	]),
	new Document('$addFields', [
		AvgTournamentSize: new Document('$cond', [
			new Document('$gt', ['$TotalTournaments', 0]),
			new Document('$divide', ['$TotalPlayers', '$TotalTournaments']),
			0
		]),
		EarningsPerTournament: new Document('$cond', [
			new Document('$gt', ['$TotalTournaments', 0]),
			new Document('$divide', ['$TotalYearlyEarnings', '$TotalTournaments']),
			0
		])
	]),
	new Document('$setWindowFields', [
		partitionBy: '$_id.Year',
		sortBy: [TotalYearlyEarnings: -1],
		output: [RankInYear: ['$rank': [:]]]
	]),
	new Document('$sort', ['_id.Year': -1, 'RankInYear': 1]),
	new Document('$project', [
		_id: 0,
		Year: '$_id.Year',
		Genre: '$_id.Genre',
		RankInYear: 1,
		TopGame: 1,
		TopGameEarnings: ['$round': ['$TopGameEarnings', 2]],
		TotalYearlyEarnings: ['$round': ['$TotalYearlyEarnings', 2]],
		TotalPlayers: 1,
		TotalTournaments: 1,
		GameCount: 1,
		AvgEarningsPerPlayer: ['$round': ['$AvgEarningsPerPlayer', 2]],
		AvgTournamentSize: ['$round': ['$AvgTournamentSize', 2]],
		EarningsPerTournament: ['$round': ['$EarningsPerTournament', 2]],
		AvgOfflinePercentage: ['$round': ['$AvgOfflinePercentage', 2]]
	]),
	new Document('$match', new Document('RankInYear', new Document('$lte', 10))),
	new Document('$skip', skip),
	new Document('$limit', pageSize)
]

// =====================================================
// PERFORMANCE TESTING
// =====================================================
def start = System.currentTimeMillis()
def results = historical.aggregate(pipeline).into([])
def end = System.currentTimeMillis()

println "Execution Time (Paginated Query): ${(end - start)} ms\n"

// Cache the results to file
def formattedResults = results.collect { doc ->
    [
        TotalPlayers: doc.TotalPlayers,
        TotalTournaments: doc.TotalTournaments,
        GameCount: doc.GameCount,
        TopGame: doc.TopGame,
        RankInYear: doc.RankInYear,
        Year: doc.Year,
        Genre: doc.Genre,
        TopGameEarnings: String.format("%.2f", doc.TopGameEarnings as Double).toDouble(),
        TotalYearlyEarnings: String.format("%.2f", doc.TotalYearlyEarnings as Double).toDouble(),
        AvgEarningsPerPlayer: String.format("%.2f", doc.AvgEarningsPerPlayer as Double).toDouble(),
        AvgTournamentSize: String.format("%.2f", doc.AvgTournamentSize as Double).toDouble(),
        EarningsPerTournament: String.format("%.2f", doc.EarningsPerTournament as Double).toDouble(),
        AvgOfflinePercentage: String.format("%.2f", doc.AvgOfflinePercentage as Double).toDouble()
    ]
}
cacheFile.text = JsonOutput.toJson(formattedResults)

results.eachWithIndex { doc, idx ->
    // Format the document with proper number formatting
    def formattedDoc = [
        TotalPlayers: doc.TotalPlayers,
        TotalTournaments: doc.TotalTournaments,
        GameCount: doc.GameCount,
        TopGame: doc.TopGame,
        RankInYear: doc.RankInYear,
        Year: doc.Year,
        Genre: doc.Genre,
        TopGameEarnings: String.format("%.2f", doc.TopGameEarnings as Double).toDouble(),
        TotalYearlyEarnings: String.format("%.2f", doc.TotalYearlyEarnings as Double).toDouble(),
        AvgEarningsPerPlayer: String.format("%.2f", doc.AvgEarningsPerPlayer as Double).toDouble(),
        AvgTournamentSize: String.format("%.2f", doc.AvgTournamentSize as Double).toDouble(),
        EarningsPerTournament: String.format("%.2f", doc.EarningsPerTournament as Double).toDouble(),
        AvgOfflinePercentage: String.format("%.2f", doc.AvgOfflinePercentage as Double).toDouble()
    ]
    
    println "Result ${idx + 1}:"
    def json = JsonOutput.toJson(formattedDoc)
    println JsonOutput.prettyPrint(json).replaceAll(/\{/, "{\n  ").replaceAll(/\}/, "\n}").replaceAll(/",/, '",\n  ').replaceAll(/(\d)\.0+([,}])/, '$1$2')
    println "---------------------------"
}

mongoClient.close()
