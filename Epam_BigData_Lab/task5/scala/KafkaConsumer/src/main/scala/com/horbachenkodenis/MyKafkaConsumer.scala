package com.horbachenkodenis

import org.apache.log4j
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, regexp_replace, window}

import scala.io.Source

object MyKafkaConsumer {

  object Conf {
    val APP_NAME = "MyConsumer"
    val MASTER = "local[*]"
    val ARGS_DELIMITER = "--"
    val MESSAGE_FORMAT: String = "\"speaker\":\"\\w*\",\"time\":\"\\d{4}-\\d{2}-\\d{2}\\d{2}:\\d{2}:\\d{2}.\\d{1,3}\",\"word\":\"\\w*\""
    val CENSORED_WORD = "censored"
  }

  def start(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new IllegalArgumentException("Arguments must be: <list of broker URLs>, <topic name> , " +
        "<censored path>, <window duration (minutes)>")
    }
    /** get args without delimiter */
    val BROKER_URL = getArgsWithNum(args, 0)
    val TOPIC_NAME = getArgsWithNum(args, 1)
    val CENSORED_PATH = getArgsWithNum(args, 2)
    val WINDOW_DURATION_MIN = getArgsWithNum(args, 3)

    val textSource = Source.fromFile(CENSORED_PATH)
    val censoredWords = textSource.getLines().toList
    textSource.close()

    val spark = org.apache.spark.sql.SparkSession.builder
      .master(Conf.MASTER)
      .appName(Conf.APP_NAME)
      .getOrCreate

    val rootLogger = log4j.Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKER_URL)
      .option("subscribe", TOPIC_NAME)
      .load()

    import spark.implicits._

    val message = data.selectExpr("CAST(value AS STRING) AS value")
      .withColumn("value", regexp_replace(col("value"),"\\s+", "") )
      .filter(col("value").rlike(Conf.MESSAGE_FORMAT))
      .selectExpr("CONCAT(SPLIT(SPLIT(value,',')[1], ':')[1], SPLIT(SPLIT(value,',')[1], ':')[2], SPLIT(SPLIT(value,',')[1], ':')[3]) AS time",
        "SPLIT(SPLIT(value,',')[2], ':')[1] AS word",
        "SPLIT(SPLIT(value,',')[0], ':')[1] AS speaker")
      .as[Message]

    val censoredMessage = message.map(message => {
      var time = message.time.replaceAll("\"", "")
      val word = message.word.replaceAll("\"", "").replaceAll("}", "")
      val speaker = message.speaker.replaceAll("\"", "")
      time = time.substring(0, 10) + " " + time.substring(10, 12) + ":" + time.substring(12, 14) + ":" + time.substring(14)
      if (censoredWords.contains(word)) {
        (time, Conf.CENSORED_WORD, speaker)
      } else
        (time, word, speaker)
    }).withColumnRenamed("_1", "time")
      .withColumnRenamed("_2", "word")
      .withColumnRenamed("_3", "speaker").as[Message]

    censoredMessage
      .groupBy(window(col("time"), WINDOW_DURATION_MIN + " minutes"), censoredMessage.col("speaker"))
      .agg(concat_ws(", ", collect_list(censoredMessage.col("word"))))
      .withColumnRenamed("concat_ws(, , collect_list(word))", "words")
      .writeStream
      .option("truncate", "false")
      .format("console")
      .outputMode("complete")
      .start()

    // without window???
    censoredMessage.filter(message => message.word.equals(Conf.CENSORED_WORD))
      .groupBy(col("speaker"))
      .count()
      .withColumnRenamed("count", "censored_count")
      .as[CensoredCount]
      .writeStream
      .option("truncate", "false")
      .format("console")
      .outputMode("complete")
      .start()

    spark.streams.awaitAnyTermination()
  }

  /** get args without delimiter */
  def getArgsWithNum(args: Array[String], num: Int): String = {
    args(num).stripPrefix(Conf.ARGS_DELIMITER).trim
  }

  case class Message(time: String, word: String, speaker: String)

  case class CensoredCount(speaker: String, censored_count: Long)
}
