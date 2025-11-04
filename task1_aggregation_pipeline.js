db.HistoricalEsportData.aggregate([
  // STAGE 1: Filter for valid and modern esports data
  {
    $match: {
      Date: { $gte: ISODate("2015-01-01") },
      Earnings: { $gt: 0 },
      Players: { $gt: 0 }
    }
  },

  // STAGE 2: Extract Year and compute per-player metrics
  {
    $addFields: {
      Year: { $year: "$Date" },
      EarningsPerPlayer: { $divide: ["$Earnings", "$Players"] }
    }
  },

  // STAGE 3: Enrich with genre and meta details from GeneralEsportData
  {
    $lookup: {
      from: "GeneralEsportData",
      localField: "Game",
      foreignField: "Game",
      as: "gameDetails"
    }
  },
  {
    $addFields: {
      Genre: { $arrayElemAt: ["$gameDetails.Genre", 0] },
      PercentOffline: { $arrayElemAt: ["$gameDetails.PercentOffline", 0] }
    }
  },
  {
    $match: { Genre: { $exists: true, $ne: null } }
  },

  // STAGE 4: First group — aggregate per Game-Year for later Top Game computation
  {
    $group: {
      _id: { Genre: "$Genre", Year: "$Year", Game: "$Game" },
      TotalEarningsPerGame: { $sum: "$Earnings" },
      TotalPlayersPerGame: { $sum: "$Players" },
      TotalTournamentsPerGame: { $sum: "$Tournaments" },
      AvgEarningsPerPlayer: { $avg: "$EarningsPerPlayer" },
      AvgOfflinePercentage: { $avg: "$PercentOffline" }
    }
  },

  // STAGE 5: Second group — aggregate per Genre-Year and find true Top Game
  {
    $sort: { "TotalEarningsPerGame": -1 } // Required before $first
  },
  {
    $group: {
      _id: { Genre: "$_id.Genre", Year: "$_id.Year" },
      TotalYearlyEarnings: { $sum: "$TotalEarningsPerGame" },
      TotalPlayers: { $sum: "$TotalPlayersPerGame" },
      TotalTournaments: { $sum: "$TotalTournamentsPerGame" },
      GameCount: { $sum: 1 },
      AvgEarningsPerPlayer: { $avg: "$AvgEarningsPerPlayer" },
      AvgOfflinePercentage: { $avg: "$AvgOfflinePercentage" },
      TopGame: { $first: "$_id.Game" },
      TopGameEarnings: { $first: "$TotalEarningsPerGame" }
    }
  },

  // STAGE 6: Derive secondary analytical metrics
  {
    $addFields: {
      AvgTournamentSize: {
        $cond: [
          { $gt: ["$TotalTournaments", 0] },
          { $divide: ["$TotalPlayers", "$TotalTournaments"] },
          0
        ]
      },
      EarningsPerTournament: {
        $cond: [
          { $gt: ["$TotalTournaments", 0] },
          { $divide: ["$TotalYearlyEarnings", "$TotalTournaments"] },
          0
        ]
      }
    }
  },

  // STAGE 7: Rank genres within each year by total yearly earnings
  {
    $setWindowFields: {
      partitionBy: "$_id.Year",
      sortBy: { TotalYearlyEarnings: -1 },
      output: {
        RankInYear: { $rank: {} }
      }
    }
  },

  // STAGE 8: Final sort for readability
  {
    $sort: {
      "_id.Year": -1,
      RankInYear: 1
    }
  },

  // STAGE 9: Final projection for clean export-ready output
  {
    $project: {
      _id: 0,
      Year: "$_id.Year",
      Genre: "$_id.Genre",
      RankInYear: 1,
      TopGame: 1,
      TopGameEarnings: { $round: ["$TopGameEarnings", 2] },
      TotalYearlyEarnings: { $round: ["$TotalYearlyEarnings", 2] },
      TotalPlayers: 1,
      TotalTournaments: 1,
      GameCount: 1,
      AvgEarningsPerPlayer: { $round: ["$AvgEarningsPerPlayer", 2] },
      AvgTournamentSize: { $round: ["$AvgTournamentSize", 2] },
      EarningsPerTournament: { $round: ["$EarningsPerTournament", 2] },
      AvgOfflinePercentage: { $round: ["$AvgOfflinePercentage", 2] }
    }
  },

  // STAGE 10: Limit (e.g., top 5 genres per year)
  { $match: { RankInYear: { $lte: 5 } } }
])
