package com.agile.cloud.automation.coursework1

import com.mongodb.client.*
import org.bson.Document
import java.util.Properties
import groovy.json.JsonOutput


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

// PAGINATION + CACHE CONFIGURATION
def pageSize = 5
def pageNumber = 1
def skip = (pageNumber - 1) * pageSize

def cacheDir = new File("cache")
if (!cacheDir.exists()) cacheDir.mkdir()

def cacheKey = "page_${pageNumber}_limit_${pageSize}"
def cacheFile = new File(cacheDir, "${cacheKey}.json")

if (cacheFile.exists() && (System.currentTimeMillis() - cacheFile.lastModified() < 300000)) {
    println "\nCache HIT — Loaded from local cache (valid for 5 mins)."
    def cachedResults = new groovy.json.JsonSlurper().parse(cacheFile)
    cachedResults.eachWithIndex { doc, idx ->
        println "Result ${idx + 1}:"
        println JsonOutput.prettyPrint(JsonOutput.toJson(doc))
        println "---------------------------"
    }
    mongoClient.close()
    return
}

println "\nCache MISS — Running Aggregation on MongoDB Cluster..."
println "Pagination Enabled → Page: ${pageNumber}, Size: ${pageSize}\n"

// AGGREGATION PIPELINE (Same query but paginated)
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
                TotalYearlyEarnings: 1,
                AvgEarningsPerPlayer: 1,
                AvgOfflinePercentage: 1,
                AvgTournamentSize: 1,
                EarningsPerTournament: 1,
                TopGameEarnings: 1
        ]),
        new Document('$match', new Document('RankInYear', new Document('$lte', 10))),
        new Document('$skip', skip),
        new Document('$limit', pageSize)
]

// PERFORMANCE MEASUREMENT
def start = System.currentTimeMillis()
def results = historical.aggregate(pipeline).into([])
def end = System.currentTimeMillis()
def execTime = end - start

println "\nExecution Time (Ex4 Paginated Query): ${execTime} ms"

// Cache the output
cacheFile.text = JsonOutput.toJson(results)

println "\n=== Paginated Top Genre Results ==="
results.eachWithIndex { doc, i ->
    println "Result ${i + 1}:"
    println JsonOutput.prettyPrint(JsonOutput.toJson(doc))
    println "---------------------------"
}

// PERFORMANCE COMPARISON SUMMARY: fixed from previous as tested multiple times
println "\n======================================"
println " PERFORMANCE COMPARISON SUMMARY"
println "======================================"
println String.format("%-15s %-20s", "Experiment", "Execution Time (ms)")
println "--------------------------------------"
println String.format("%-15s %-20s", "Ex2 (Local CSV)", "≈ 1862 ms")
println String.format("%-15s %-20s", "Ex3 (Mongo Cloud + Indexing)", "≈ 705 ms")
println String.format("%-15s %-20s", "Ex4 (Paged + Cache)", "${execTime} ms")
println "--------------------------------------"
println "Scalability improved by limiting data transfer + caching\n"

mongoClient.close()
