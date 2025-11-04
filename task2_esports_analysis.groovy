package com.agile.cloud.automation.coursework1

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import groovy.json.JsonOutput

class EsportEvent {
    String game
    LocalDate date
    Integer year
    Double earnings
    Integer players
    Integer tournaments
    Double earningsPerPlayer
    String genre
    Double percentOffline
}

class EsportsAnalysis {

    static List<String[]> parseCsv(String content) {
        content.split('\n').collect { line ->
            def result = []
            def current = new StringBuilder()
            def inQuotes = false
            line.each { ch ->
                if (ch == '"') inQuotes = !inQuotes
                else if (ch == ',' && !inQuotes) {
                    result << current.toString().trim()
                    current = new StringBuilder()
                } else current.append(ch)
            }
            result << current.toString().trim()
            result.collect { it.replaceAll('^"|"$', '').trim() }
        }
    }

    static Double safeToDouble(String s) {
        try { return s ? s.toDouble() : 0.0 } catch (e) { return 0.0 }
    }

    static Integer safeToInt(String s) {
        try { return s ? s.toInteger() : 0 } catch (e) { return 0 }
    }

    static Map processData() {
        def fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        def histRows = parseCsv(new File("datasets/HistoricalEsportData.csv").text)
        def genRows  = parseCsv(new File("datasets/GeneralEsportData.csv").text)

        // Correct mapping for GeneralEsportData:
        // 0: Game, 1: ReleaseDate, 2: Genre, 3: TotalEarnings, 4: OfflineEarnings, 5: PercentOffline, 6: TotalPlayers, 7: TotalTournaments
        def general = genRows.tail().collectEntries { row ->
            def offlineEarnings = safeToDouble(row[4])
            def totalEarnings = safeToDouble(row[3])
            def percentOffline = totalEarnings > 0 ? offlineEarnings / totalEarnings : 0.0
            [(row[0]): [
                genre: row[2],
                percentOffline: percentOffline  // Calculate actual percentage from earnings
            ]]
        }

        def events = histRows.tail().collect { row ->
            def date = LocalDate.parse(row[0], fmt)
            def game = row[1]
            def earnings = safeToDouble(row[2])
            def players = safeToInt(row[3])
            def tournaments = safeToInt(row[4])
            if (date >= LocalDate.of(2015, 1, 1) && earnings > 0 && players > 0 && general[game]) {
                def g = general[game]
                return new EsportEvent(
                    game: game,
                    date: date,
                    year: date.year,
                    earnings: earnings,
                    players: players,
                    tournaments: tournaments,
                    earningsPerPlayer: earnings / players,
                    genre: g.genre,
                    percentOffline: g.percentOffline
                )
            }
            return null
        }.findAll { it != null }

        // 🔹 Group by Genre-Year-Game (use composite key for stability)
        def gameYearMap = events.groupBy { "${it.genre}|${it.year}|${it.game}" }.collectEntries { key, list ->
            def genre = key.split('\\|')[0]
            def year = key.split('\\|')[1].toInteger()
            def game = key.split('\\|')[2]
            [
                key,
                [
                    genre: genre,
                    year: year,
                    game: game,
                    totalEarnings: list.sum { it.earnings },
                    totalPlayers: list.sum { it.players },
                    totalTournaments: list.sum { it.tournaments },
                    avgEarningsPerPlayer: list.collect { it.earningsPerPlayer }.sum() / list.size(),
                    avgOfflinePercentage: list.collect { it.percentOffline }.sum() / list.size()
                ]
            ]
        }

        // 🔹 Aggregate per Genre-Year
        def genreYearMap = gameYearMap.values().groupBy { "${it.genre}|${it.year}" }.collectEntries { k, games ->
            def genre = k.split('\\|')[0]
            def year = k.split('\\|')[1].toInteger()
            def sortedGames = games.sort { -it.totalEarnings }
            def top = sortedGames.first()
            [
                k,
                [
                    genre: genre,
                    year: year,
                    totalYearlyEarnings: games.sum { it.totalEarnings },
                    totalPlayers: games.sum { it.totalPlayers },
                    totalTournaments: games.sum { it.totalTournaments },
                    gameCount: games.size(),
                    avgEarningsPerPlayer: games.collect { it.avgEarningsPerPlayer }.sum() / games.size(),
                    avgOfflinePercentage: games.collect { it.avgOfflinePercentage }.sum() / games.size(),
                    topGame: top.game,
                    topGameEarnings: top.totalEarnings
                ]
            ]
        }

        // 🔹 Add derived metrics & rank
        def byYear = genreYearMap.values().groupBy { it.year }
        byYear.each { year, list ->
            def sorted = list.sort { -it.totalYearlyEarnings }
            sorted.eachWithIndex { val, idx ->
                val.rankInYear = idx + 1
                val.avgTournamentSize = val.totalTournaments > 0 ? val.totalPlayers / val.totalTournaments : 0
                val.earningsPerTournament = val.totalTournaments > 0 ? val.totalYearlyEarnings / val.totalTournaments : 0
            }
        }

        return genreYearMap
    }

    static void printResults(Map results) {
        def sorted = results.values().sort { a, b -> b.year <=> a.year ?: a.rankInYear <=> b.rankInYear }
        def grouped = sorted.groupBy { it.year }

        grouped.each { year, list ->
            list.take(5).each { v ->
                def obj = [
                    TotalPlayers: v.totalPlayers,
                    TotalTournaments: v.totalTournaments,
                    GameCount: v.gameCount,
                    TopGame: v.topGame,
                    RankInYear: v.rankInYear,
                    Year: v.year,
                    Genre: v.genre,
                    TopGameEarnings: Math.round(v.topGameEarnings * 100) / 100,
                    TotalYearlyEarnings: Math.round(v.totalYearlyEarnings * 100) / 100,
                    AvgEarningsPerPlayer: Math.round(v.avgEarningsPerPlayer * 100) / 100,
                    AvgTournamentSize: Math.round(v.avgTournamentSize * 100) / 100,
                    EarningsPerTournament: Math.round(v.earningsPerTournament * 100) / 100,
                    AvgOfflinePercentage: Math.round(v.avgOfflinePercentage * 100) / 100
                ]
                println JsonOutput.prettyPrint(JsonOutput.toJson(obj))
                println()
            }
        }
    }
}

def res = EsportsAnalysis.processData()
EsportsAnalysis.printResults(res)
