package com.horbachenkodenis

import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreTopPlayersCounter {

  object Conf {
    val APP_NAME = "SparkCoreTopPlayersCounter"
    val MASTER = "local[*]"
    val TESTING_MEMORY = "spark.testing.memory"
    val TESTING_MEMORY_CAP = "2147480000"

    val AWARDS_FILE = "AwardsPlayers.csv"
    val MASTER_FILE = "Master.csv"
    val SCORING_FILE = "Scoring.csv"
    val TEAMS_FILE = "Teams.csv"
    val INPUT_DIRECTORY_NAME = "input/"
    val INPUT_VAL_DELIMITER = ","

    val MASTER_PLAYER_ID_COL = 0
    val MASTER_COACH_ID_COL = 1
    val MASTER_PlayerHOF_ID_COL = 2
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
    val OUTPUT_PATH = "output/RDD/"
    val OUTPUT_FILE_NAME = "Top10Player"
  }

  def spaceSkipper(a:String): Int = {
    if (a != null && !a.isEmpty) a.toInt
    else 0
  }

  def main(args: Array[String]) {

    val conf:SparkConf = new SparkConf()
      .setAppName(Conf.APP_NAME)
      .setMaster(Conf.MASTER)
      .set(Conf.TESTING_MEMORY, Conf.TESTING_MEMORY_CAP)
    val sc:SparkContext = new SparkContext(conf)

    /**cluster input */
    val inputAwardsPlayerRdd = sc.textFile(args(0) +"/" + Conf.AWARDS_FILE)
      .mapPartitionsWithIndex{(index, iterator) => if (index == 0) iterator.drop(1) else iterator}
    val inputMasterRdd = sc.textFile(args(0) + "/" + Conf.MASTER_FILE)
      .mapPartitionsWithIndex{(index, iterator) => if (index == 0) iterator.drop(1) else iterator}
    val inputScoringRdd = sc.textFile(args(0) + "/" + Conf.SCORING_FILE)
      .mapPartitionsWithIndex{(index, iterator) => if (index == 0) iterator.drop(1) else iterator}
    val inputTeamsRdd = sc.textFile(args(0) + "/" + Conf.TEAMS_FILE)

//        /**local input */
//        val inputAwardsPlayerRdd = sc.textFile(Conf.INPUT_DIRECTORY_NAME + Conf.AWARDS_FILE)
//          .mapPartitionsWithIndex{(index, iterator) => if (index == 0) iterator.drop(1) else iterator}
//        val inputMasterRdd = sc.textFile(Conf.INPUT_DIRECTORY_NAME + Conf.MASTER_FILE)
//          .mapPartitionsWithIndex{(index, iterator) => if (index == 0) iterator.drop(1) else iterator}
//        val inputScoringRdd = sc.textFile(Conf.INPUT_DIRECTORY_NAME + Conf.SCORING_FILE)
//          .mapPartitionsWithIndex{(index, iterator) => if (index == 0) iterator.drop(1) else iterator}
//        val inputTeamsRdd = sc.textFile(Conf.INPUT_DIRECTORY_NAME + Conf.TEAMS_FILE)

    /**get tuple: playerId | firstNameAndLastName */
    val playerNamesRdd = inputMasterRdd.map(line => {
      val values = line.split(Conf.INPUT_VAL_DELIMITER)
      var playerId = ""
      for (id: String <- Array(values(Conf.MASTER_PLAYER_ID_COL), values(Conf.MASTER_COACH_ID_COL).dropRight(1),
        values(Conf.MASTER_PlayerHOF_ID_COL).dropRight(1))){
        if (id !="") playerId = id
      }
      new Tuple2[String, String](playerId, String.join(" ", values(Conf.MASTER_PLAYER_F_NAME_COL),
        values(Conf.MASTER_PLAYER_L_NAME_COL)))
    })

    /**get tuple: playerId | sumOfAward */
    val playerAwardsRdd = inputAwardsPlayerRdd.map(line => {
      val values = line.split(Conf.INPUT_VAL_DELIMITER)
      new Tuple2[String, Int](values(Conf.AWARDS_PLAYER_ID_COL), 1)
    }).reduceByKey(_+_)

    /**get tuple: playerId | sumOfGoals */
    val goalsRdd = inputScoringRdd.map(line => {
      val values = line.split(Conf.INPUT_VAL_DELIMITER)
      try {
        new Tuple2[String, Int](values(Conf.SCORING_PLAYER_ID_COL), spaceSkipper(values(Conf.SCORING_GOALS_COL)))
      }catch { case ex: ArrayIndexOutOfBoundsException =>
        new Tuple2[String, Int](values(Conf.SCORING_PLAYER_ID_COL), 0)
      }
    }).reduceByKey(_+_)

    /**get tuple: teamId | teamName */
    val teamsRdd = inputTeamsRdd.map(line => {
      val values = line.split(Conf.INPUT_VAL_DELIMITER)
      new Tuple2[String, String](values(Conf.TEAM_ID_COL), values(Conf.TEAM_NAME_COL))
    })

    /**get tuple: teamId | playerId */
    val playerTeamId = inputScoringRdd.map(line => {
      val values = line.split(Conf.INPUT_VAL_DELIMITER)
      new Tuple2[String, String](values(Conf.SCORING_TEAM_ID_COL), values(Conf.SCORING_PLAYER_ID_COL))
    })
      .distinct()

    /**get tuple: playerId | listTeamNames */
    val playerTeams = playerTeamId.join(teamsRdd)
      .map(column => (column._2._1, column._2._2))
      .distinct()
      .reduceByKey(_ + Conf.INPUT_VAL_DELIMITER + _)

    /**get result RDD */
    val playerResult = playerNamesRdd.leftOuterJoin(playerAwardsRdd)
      .leftOuterJoin(goalsRdd)
      .leftOuterJoin(playerTeams)

    /**get result [String] */
    val finalRdd = playerResult
      .sortBy(column => column._2._1._2, false)
      .map(column => {
        column._2._1._1._1 + Conf.OUTPUT_DELIMITER +
          column._2._1._1._2.getOrElse(0) + Conf.OUTPUT_DELIMITER +
          column._2._1._2.getOrElse(0) + Conf.OUTPUT_DELIMITER +
          column._2._2.getOrElse("No team")
      })
      .coalesce(1)


    val top10Players = sc.parallelize(sc.parallelize(Array("Name | Awards | Goals | Teams "))
      .coalesce(1)
      .union(finalRdd)
      .coalesce(1)
      .take(Conf.TAKE_TOP_PLAYERS + 1))


//        /**local output */
//        /**get output */
//        top10Players.coalesce(1).saveAsTextFile(Conf.OUTPUT_PATH +
//           Conf.OUTPUT_FILE_NAME + ".txt")
//        top10Players.coalesce(1).saveAsObjectFile(Conf.OUTPUT_PATH +
//          Conf.OUTPUT_FILE_NAME + ".obj")
//        top10Players.map (x => (x.split("|")(0), x)).coalesce(1)
//            .saveAsSequenceFile(Conf.OUTPUT_PATH + Conf.OUTPUT_FILE_NAME + ".sq")

    /**cluster output */
    /**get output */
    top10Players.coalesce(1)
      .saveAsTextFile(args(1) + Conf.OUTPUT_PATH + Conf.OUTPUT_FILE_NAME + ".txt")
    top10Players.coalesce(1)
      .saveAsObjectFile(args(1) + Conf.OUTPUT_PATH + Conf.OUTPUT_FILE_NAME + ".obj")
    top10Players.map (x => (x.split("|")(0), x)).coalesce(1)
      .saveAsSequenceFile(args(1) + Conf.OUTPUT_PATH + Conf.OUTPUT_FILE_NAME + ".sq")

    //    Thread.sleep(10 * 60 * 1000) // 10 minutes

//        top10Players.coalesce(1).foreach(println)
    sc.stop()
  }

}
