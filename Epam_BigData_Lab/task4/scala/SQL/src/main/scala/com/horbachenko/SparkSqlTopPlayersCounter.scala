package com.horbachenkodenis

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlTopPlayersCounter {

  object Conf {
    val APP_NAME = "SparkSqlTopPlayersCounter"
    val MASTER = "local[*]"
    val TESTING_MEMORY = "spark.testing.memory"
    val TESTING_MEMORY_CAP = "2147480000"
    val INPUT_FILE_TYPE = "csv"
    val INPUT_FILE_DIRECTORY_LOCAL = "input/"
    val INPUT_FILE_DIRECTORY_CLUSTER = "/"
      val AWARDS_FILE = "AwardsPlayers.csv"
      val MASTER_FILE = "Master.csv"
      val SCORING_FILE = "Scoring.csv"
      val TEAMS_FILE = "Teams.csv"

    val TAKE_TOP_PLAYERS = 10

    val OUTPUT_DIRECTORY = "output/SQL/"
    val OUTPUT_CSV = "Top10Player.csv"
    val OUTPUT_PARQUET = "Top10Player.parquet"
  }

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

  /** get DataFrame from file */
  private def createDataFrame(path: String): Dataset[Row] = {
    spark.read.format(Conf.INPUT_FILE_TYPE)
      .option("header", "true")
      .load(path)
  }

  def createSqlQuery(str: String): Dataset[Row] = {
    spark.sql(str)
  }

  def main(args: Array[String]): Unit = {

    /** create player DF */
    createDataFrame(
      args(0) +
        Conf.INPUT_FILE_DIRECTORY_CLUSTER + Conf.MASTER_FILE)
      .createOrReplaceTempView("allPlayer")

    /** create award DF */
    createDataFrame(
      args(0) +
        Conf.INPUT_FILE_DIRECTORY_CLUSTER + Conf.AWARDS_FILE)
      .createOrReplaceTempView("allAwards")

    /** create goal DF */
    createDataFrame(
      args(0) +
        Conf.INPUT_FILE_DIRECTORY_CLUSTER + Conf.SCORING_FILE)
      .createOrReplaceTempView("allGoals")

    /** create team DF */
    createDataFrame(
      args(0) +
        Conf.INPUT_FILE_DIRECTORY_CLUSTER + Conf.TEAMS_FILE)
      .select("tmID", "name")
      .createOrReplaceTempView("allTeams")

    /** get DF: playerId | playerName */
    createSqlQuery("select playerID, concat(firstName,' ',lastName) as Name from allPlayer").toDF()
      .createOrReplaceTempView("playerData")

    /** get DF: playerId | sumOfAwardWithoutNull */
    createSqlQuery("Select playerID , COUNT(award) as award_cnt from allAwards " +
      "Group by playerID ")
      .createOrReplaceTempView("awardsTmp")

    /** get DF: playerId | sumOfAwardWithNull */
    createSqlQuery("SELECT " +
      " p.playerID, a.award_cnt"
      + " From playerData p "
      + " LEFT JOIN awardsTmp a ON p.playerID = a.playerID "
    )
      .createOrReplaceTempView("awards")

    /** get DF: playerId | sumOfGoals */
    createSqlQuery("SELECT playerID , SUM(G) as goals FROM allGoals " +
      " Group by playerID")
      .createOrReplaceTempView("goals")

    /** get DF: playerId | listTeamID */
    createSqlQuery("SELECT distinct" +
      " g.playerID"
      + " , concat_ws(', ', collect_set(t.name)) as teams "
      + " From allGoals g "
      + " JOIN allTeams t ON g.tmID = t.tmID "
      + " GROUP BY g.playerID"
    ).createOrReplaceTempView("teams")

    import spark.implicits._

    /** get final DF */
    val finalSelect = createSqlQuery(
      "SELECT distinct Name, cast(award_cnt as int) as awards, cast(goals as int), teams From playerData p, awards a, goals g, teams t "
        + "WHERE  p.playerID=g.playerID and p.playerID=a.playerID and p.playerID=t.playerID "
        + "order by goals desc "
        + "limit " + Conf.TAKE_TOP_PLAYERS)

    /** create header*/
   val output = finalSelect.map(x => {
     var award = 0
     val tableAward = x.getAs[Int]("awards")
     if (tableAward!=null)
       award = tableAward
     (x.getAs[String]("Name"), award, x.getAs[Int]("goals"),
      x.getAs[String]("teams"))
    }).withColumnRenamed("_1", "Name")
      .withColumnRenamed("_2", "Awards")
      .withColumnRenamed("_3", "Goals")
      .withColumnRenamed("_4", "Teams")

//    output.show()

    /** output result */
//    /**local output */
//    output.write.option("header","true").csv(Conf.OUTPUT_DIRECTORY + Conf.OUTPUT_CSV)
//    output.write.mode("overwrite").format("parquet")
//      .save(Conf.OUTPUT_DIRECTORY + Conf.OUTPUT_PARQUET)

    /**cluster output */
    output.write.option("header","true").csv(args(1) + Conf.OUTPUT_DIRECTORY + Conf.OUTPUT_CSV)
    output.write.mode("overwrite").format("parquet")
      .save(args(1) + Conf.OUTPUT_DIRECTORY + Conf.OUTPUT_PARQUET)

    sc.stop()
  }
}
