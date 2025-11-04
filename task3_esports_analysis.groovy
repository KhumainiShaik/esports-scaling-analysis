package com.agile.cloud.automation.coursework1

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

// =====================================================
// SCALABILITY OPTIMIZATION: CREATE INDEXES
// =====================================================
println "Creating indexes for optimized aggregation..."

historical.createIndex(new Document("Date", 1))
historical.createIndex(new Document("Earnings", 1))
historical.createIndex(new Document("Players", 1))
historical.createIndex(new Document("Game", 1))
general.createIndex(new Document("Game", 1))

println "Indexes created successfully.\n"

// =====================================================
// ️AGGREGATION PIPELINE
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
    new Document('$match', new Document('RankInYear', new Document('$lte', 5)))
]

// =====================================================
// Execute Aggregation and Format Output
// =====================================================
def results = historical.aggregate(pipeline).into([])

results.each { doc ->
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
    
    def jsonOutput = new JsonOutput()
    def json = jsonOutput.toJson(formattedDoc)
    // Configure JsonOutput to not use scientific notation
    println jsonOutput.prettyPrint(json).replaceAll(/\{/, "{\n  ").replaceAll(/\}/, "\n}").replaceAll(/",/, '",\n  ').replaceAll(/(\d)\.0+([,}])/, '$1$2')
    println()
}

mongoClient.close()
