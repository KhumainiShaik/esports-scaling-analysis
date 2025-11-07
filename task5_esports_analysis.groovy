package com.agile.cloud.automation.coursework1

import com.mongodb.client.*
import org.bson.Document
import java.util.Properties
import groovy.json.JsonOutput
import java.util.concurrent.TimeUnit



// ---------- Load MongoDB credentials ----------
def props = new Properties()
new File("env/mongo.properties").withInputStream { props.load(it) }

def username = props['mongodb.username']
def password = props['mongodb.password']
def cluster = props['mongodb.cluster']
def databaseName = props['mongodb.database']
def histColl = props['mongodb.collections.historical']
def genColl = props['mongodb.collections.general']

def uri = "mongodb+srv://${username}:${password}@${cluster}/?retryWrites=true&w=majority"
MongoClient mongoClient = MongoClients.create(uri)
def db = mongoClient.getDatabase(databaseName)
def historical = db.getCollection(histColl)
def general = db.getCollection(genColl)

historical.createIndex(new Document("Date", 1))
historical.createIndex(new Document("Game", 1))
general.createIndex(new Document("Game", 1))

println "\nIndexes verified/created: Date, Game"


// SCALE DATASET (5x and 10x)
def generateSyntheticData = { scaleFactor ->
    def count = historical.countDocuments()
    def newDocs = []
    println "\nScaling dataset by ${scaleFactor}x..."
    def originalDocs = historical.find().limit(2000)
    originalDocs.each { doc ->
        (1..scaleFactor).each { i ->
            def clone = new Document(doc)
            clone.put("_id", new org.bson.types.ObjectId())
            clone.put("SyntheticBatch", i)
            newDocs << clone
        }
    }
    if (historical.countDocuments(new Document("SyntheticBatch", new Document("\$exists", true))) == 0) {
        historical.insertMany(newDocs)
        println "Inserted ${newDocs.size()} synthetic records for ${scaleFactor}x dataset."
    } else {
        println "Synthetic records already exist for ${scaleFactor}x — skipping insertion."
    }
}

// Generate 5x and 10x synthetic data
generateSyntheticData(5)
generateSyntheticData(10)

// PAGINATED AGGREGATION FUNCTION
def runAggregation = { title, pageSize = 5, pageNumber = 1 ->
    println "\nRunning aggregation for: ${title} (Page ${pageNumber}, Size ${pageSize})"
    def skip = (pageNumber - 1) * pageSize
    def start = System.nanoTime()

    def pipeline = [
            new Document('$match', new Document('Date', new Document('$gte', new Date(124,0,1)))
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
            new Document('$setWindowFields', [
                    partitionBy: '$_id.Year',
                    sortBy: [TotalYearlyEarnings: -1],
                    output: [RankInYear: ['$rank': [:]]]
            ]),
            new Document('$match', new Document('RankInYear', new Document('$lte', 5))),
            new Document('$skip', skip),
            new Document('$limit', pageSize)
    ]

    def results = historical.aggregate(pipeline).into([])
    def end = System.nanoTime()
    def durationMs = TimeUnit.NANOSECONDS.toMillis(end - start)

    println "Execution Time: ${durationMs} ms | Records Returned: ${results.size()}"

    return [time: durationMs, results: results]
}

// RUN BASELINE + SCALED DATASETS
def baselinePerf = runAggregation("Baseline Dataset (x1)")
def scaled5xPerf = runAggregation("Scaled Dataset (x5)")
def scaled10xPerf = runAggregation("Scaled Dataset (x10)")

// PERFORMANCE
println "\n=============================="
println " Performance Comparison Table (Top 5 genres per page)"
println "=============================="
println String.format("%-30s %-15s %-20s", "Experiment", "Dataset Size", "Execution Time (ms)")
println "------------------------------------------------------------------"
println String.format("%-30s %-15s %-20s", "Baseline (Mongo Cloud + Paging)", "x1", baselinePerf.time)
println String.format("%-30s %-15s %-20s", "Scaled Dataset (Paging)", "x5", scaled5xPerf.time)
println String.format("%-30s %-15s %-20s", "Scaled Dataset (Paging)", "x10", scaled10xPerf.time)
println "------------------------------------------------------------------"

mongoClient.close()
