package com.horbachenkodenis

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.Random

/** Class represents Speaker with name and lastMessageTime */
class Speaker(name: String, time: LocalDateTime) {

  val timeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
  var lastMessageTime: LocalDateTime = time

  def getName: String = {
    name
  }

  def getTime: String = {
    lastMessageTime = lastMessageTime.plusMinutes(Random.nextInt(10))
    lastMessageTime = lastMessageTime.plusSeconds(Random.nextInt(59))
    lastMessageTime.format(timeFormat)
  }
}
