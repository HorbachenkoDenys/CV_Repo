package com.horbachenkodenis

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkDataSetTopPlayersCounter {

  object Conf {
    val APP_NAME = "SparkDataSetTopPlayersCounter"
    val MASTER = "local[*]"
    val TESTING_MEMORY = "spark.testing.memory"
    val TESTING_MEMORY_CAP = "2147480000"

    val AWARDS_FILE = "AwardsPlayers.csv"
    val MASTER_FILE = "Master.csv"
    val SCORING_FILE = "Scoring.csv"
    val TEAMS_FILE = "Teams.csv"
    val INPUT_DIRECTORY_NAME_LOCAL = "input/"
    val INPUT_DIRECTORY_NAME_CLUSTER = "/"
    val INPUT_VAL_DELIMITER = ","

    val MASTER_PLAYER_ID_COL = 0
    val MASTER_PLAYER_F_NAME_COL = 3
    val MASTER_PLAYER_L_NAME_COL = 4

    val AWARDS_PLAYER_ID_COL = 0

    val SCORING_PLAYER_ID_COL = 0
    val SCORING_TEAM_ID_COL = 3
    val SCORING_GOALS_COL = 7

    val TEAM_ID_COL = 2
    val TEAM_NAME_COL = 18

    val TAKE_TOP_PLAYERS = 10

    val OUTPUT_DELIMITER = "|"
    val OUTPUT_PATH = "output/DS/"
    val OUTPUT_FILE_NAME = "Top10Player"
  }

  /** Class that represents data */
  case class PlayerData(playerID: String, name: String)

  case class AwardsData(playerID: String, numAwards: Int)

  case class GoalsData(playerID: String, numGoals: Int)

  case class TeamData(playerID: String, teams: String)

  case class TeamNamesData(teamID: String, teamName: String)

  case class PlayerAwardsGoals(playerID: String, name: String, awards: Int = 0, goals: Int = 0)

  case class Player(name: String, awards: Int = 0, goals: Int = 0, teams: String = "None")

  /** Omit initializing error */
  val conf: SparkConf = new SparkConf()
    .setAppName(Conf.APP_NAME)
    .setMaster(Conf.MASTER)
    .set(Conf.TESTING_MEMORY, Conf.TESTING_MEMORY_CAP)
  val sc: SparkContext = new SparkContext(conf)

  private val spark = org.apache.spark.sql.SparkSession.builder
    .master(Conf.MASTER)
    .appName(Conf.APP_NAME)
    .getOrCreate


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("Arguments: [input dir path], [output dir path]")
    }

    import spark.implicits._

    /** Get DS top n playerID | sumOfGoals */
    val goals = getDS(
      args(0) +
        Conf.INPUT_DIRECTORY_NAME_CLUSTER + Conf.SCORING_FILE)
      .filter(line => line.getString(Conf.SCORING_GOALS_COL) != null)
      .map({ line =>
        var goals = 0
        if (line.getString(Conf.SCORING_GOALS_COL) != null && line.getString(Conf.SCORING_GOALS_COL) != "")
          goals = line.getString(Conf.SCORING_GOALS_COL).toInt
        line.getString(Conf.SCORING_PLAYER_ID_COL) -> goals
      })
      .groupBy("_1")
      .agg(Map("_2" -> "sum"))
      .map(line => GoalsData(line.getString(Conf.SCORING_PLAYER_ID_COL), line.getAs[Long]("sum(_2)").toInt))
      .sort($"numGoals".desc)
      .limit(Conf.TAKE_TOP_PLAYERS)

    /** Get broadcast DS top n playerID | sumOfGoals */
    val topPlayersGoals = spark.sparkContext.broadcast(goals.collect())

    /** Get broadcast DS teamID | teamName */
    val teamNames = spark.sparkContext.broadcast(getDS(
      args(0) +
        Conf.INPUT_DIRECTORY_NAME_CLUSTER + Conf.TEAMS_FILE)
      .map(line => TeamNamesData(line.getString(Conf.TEAM_ID_COL), line.getString(Conf.TEAM_NAME_COL))))

    /** Get DS playerID | playerName */
    val players = getDS(
      args(0) +
        Conf.INPUT_DIRECTORY_NAME_CLUSTER + Conf.MASTER_FILE)
      .filter(master => topPlayersGoals.value.map(topPlayers => topPlayers.playerID)
        .contains(master.getString(Conf.MASTER_PLAYER_ID_COL)))
      .map(line => PlayerData(line.getString(Conf.MASTER_PLAYER_ID_COL), line.getString(Conf.MASTER_PLAYER_F_NAME_COL)
        + " " + line.getString(Conf.MASTER_PLAYER_L_NAME_COL)))

    /** Get DS playerID | sumOfAwards */
    val awards = getDS(
      args(0) +
        Conf.INPUT_DIRECTORY_NAME_CLUSTER + Conf.AWARDS_FILE)
      .filter(awards => topPlayersGoals.value.map(topPlayers => topPlayers.playerID)
        .contains(awards.getString(Conf.AWARDS_PLAYER_ID_COL)))
      .groupByKey(_.getString(Conf.AWARDS_PLAYER_ID_COL))
      .count()
      .map(line => AwardsData(line._1, (line._2).toInt))

    /** Get DS playerID | TeamID */
    val teams = getDS(
      args(0) +
      Conf.INPUT_DIRECTORY_NAME_CLUSTER + Conf.SCORING_FILE)
      .filter(teams => topPlayersGoals.value.map(topPlayers => topPlayers.playerID)
        .contains(teams.getString(Conf.SCORING_PLAYER_ID_COL)))
      .map(line => line.getString(Conf.SCORING_PLAYER_ID_COL) -> line.getString(Conf.SCORING_TEAM_ID_COL))
      .joinWith(teamNames.value.alias("teamNames"), col("_2") === col("teamNames.teamID"))
      .map(line => line._1._1 -> line._2.teamName)
      .distinct()
      .groupByKey(_._1)
      .reduceGroups((a, b) => (a._1, a._2 + ", " + b._2)).map(_._2)
      .withColumnRenamed("_1", "playerID").withColumnRenamed("_2", "teams")
        .as[TeamData]

    /** Get DS playerID, playerName, sumOfAwards */
    val playerAwards: Dataset[PlayerAwardsGoals] = players
      .join(awards.alias("awards"), Seq("playerID"), "left_outer")
      .withColumn("awards", col("awards.numAwards"))
      .map(row => PlayerAwardsGoals(
        row.getAs[String]("playerID"),
        row.getAs[String]("name"),
        row.getAs[Int]("awards")
      ))
//
    /** Get DS playerID, playerName, sumOfAwards, sumOfGoals */
    val playerAwardsGoals = playerAwards
      .join(goals.alias("goals"), Seq("playerID"))
      .withColumn("goals", col("goals.numGoals"))
      .map(row => PlayerAwardsGoals(
        row.getAs[String]("playerID"),
        row.getAs[String]("name"),
        row.getAs[Int]("awards"),
        row.getAs[Int]("goals")
      ))

    /** Get DS playerID, playerName, sumOfAwards, sumOfGoals, SeqOfTeamName */
    val result = playerAwardsGoals
      .join(teams.alias("teams"), Seq("playerID"))
      .map(row => Player(
        row.getAs[String]("name"),
        row.getAs[Int]("awards"),
        row.getAs[Int]("goals"),
        row.getAs[String]("teams")
      )).orderBy($"goals".desc).coalesce(1)

//            result.show()
    result.withColumn("teams", col("teams")
      .cast("string"))
      .write.option("header", "true")
      .csv(
        args(1) +
          Conf.OUTPUT_PATH + Conf.OUTPUT_FILE_NAME + ".csv")

    result
      .toJSON
      .write
      .format("json")
      .save(
        args(1) +
          Conf.OUTPUT_PATH + Conf.OUTPUT_FILE_NAME + ".json")

    spark.stop()
  }

  /** get DS from given path */
  def getDS(path: String): Dataset[Row] = {
    spark.read
      .format("csv")
      .option("header", true)
      .load(path)
  }
}
