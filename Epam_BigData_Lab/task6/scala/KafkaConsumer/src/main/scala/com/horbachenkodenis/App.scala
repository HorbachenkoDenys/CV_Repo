package com.horbachenkodenis

/** @author horbachenkodenis */
object App {
  def main(args: Array[String]):Unit = {
    MyKafkaConsumer.start(args)
  }
}
