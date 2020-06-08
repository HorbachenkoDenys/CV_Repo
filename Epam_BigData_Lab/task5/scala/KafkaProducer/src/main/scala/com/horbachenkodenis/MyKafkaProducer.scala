package com.horbachenkodenis

import java.time.LocalDateTime
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.Source
import scala.util.Random

/** Class send correct and incorrect data to broker by given URL, topicName, speakers and dictionary */
object MyKafkaProducer {

  val ARGS_DELIMITER = "--"

  val PRODUCE_NUM_MESSAGE = 8
  val PRODUCE_NUM_INCORRECT_MESSAGE = 2

  var time: LocalDateTime = LocalDateTime.now()

  def start(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new IllegalArgumentException("Arguments must be: <list of broker URLs>, <topic name> , " +
        "<list of speaker names>, <dictionary path>")
    }

    /** get args without delimiter */
    val BROKER_URL = getArgsWithNum(args, 0)
    val TOPIC_NAME = getArgsWithNum(args, 1)
    val SPEAKERS_NAME: Array[String] = getArgsWithNum(args, 2).split(",")
    val DICTIONARY_PATH = getArgsWithNum(args, 3)

    val textSource = Source.fromFile(DICTIONARY_PATH)
    val words = textSource.getLines().toList
    textSource.close()
    val speakers = SPEAKERS_NAME.map(speaker => new Speaker(speaker, time))

    val properties = new Properties()
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    val correctDataThread = new Thread(new Runnable {
      override def run(): Unit = sendMessages(producer, TOPIC_NAME, speakers, words)
    })

    val inCorrectDataThread = new Thread(new Runnable {
      override def run(): Unit = sendIncorrectMessages(producer, TOPIC_NAME)
    })

    correctDataThread.start()
    inCorrectDataThread.start()
    correctDataThread.join()
    inCorrectDataThread.join()
    producer.close()
  }
/** send data to broker */
  def sendMessages(producer: KafkaProducer[String, String], TOPIC_NAME: String, speakers: Array[Speaker], words: List[String]): Unit = {
    for (_ <- 0 until PRODUCE_NUM_MESSAGE) {
      //??? Second time make Speaker?
      val speaker = new Speaker(speakers(Random.nextInt(speakers.length)).getName, time)
      val message = new Message(speaker.getName, speaker.getTime, words(Random.nextInt(words.length)))
      val record = new ProducerRecord(TOPIC_NAME, message.messageId, message.toString)
      producer.send(record)
      Thread.sleep(2000)
    }
  }
  /** send incorrect data to broker */
  def sendIncorrectMessages(producer: KafkaProducer[String, String], TOPIC_NAME: String): Unit = {
    for (i <- 1 to PRODUCE_NUM_INCORRECT_MESSAGE) {
      val record = new ProducerRecord(TOPIC_NAME, "Incorrect key " + i, "Incorrect value " + i)
      producer.send(record)
      Thread.sleep(2000)
    }
  }
  /** get args without delimiter */
  def getArgsWithNum(args: Array[String], num: Int): String = {
    args(num).stripPrefix(ARGS_DELIMITER).trim
  }
}
