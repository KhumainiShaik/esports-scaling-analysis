package com.agile.cloud.automation.coursework1

import com.mongodb.client.*
import org.bson.Document
import java.util.Properties
import groovy.json.JsonOutput

class Task3EsportsAnalysis {
    static void main(String[] args) {
        // ---------- Load MongoDB credentials ----------
        def props = new Properties()
        new File("./src/main/resources/mongo.properties").withInputStream { props.load(it) }

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

        // Rest of your existing code here, everything from "SCALABILITY OPTIMIZATION" onwards...
        // (The pipeline definition and execution remain exactly the same)

        mongoClient.close()
    }
}