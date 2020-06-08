package com.horbachenkodenis

/** Class represents message sanded by Producer */
class Message(speaker: String, time: String, word: String) {

  val messageId: String = speaker + " " + time

  override def toString: String = "{\n" +
    "\t\"speaker\": \"" + speaker + "\",\n" +
    "\t\"time\": \"" + time + "\",\n" +
    "\t\"word\": \"" + word + "\"\n" +
    "}"

}
