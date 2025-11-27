package com.herminiogarcia.shexml.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

import scala.collection.JavaConverters._

class KafkaTestDataEmitter(server: String, topic: String) {

  private val kafkaEmitter = createKafkaEmitter()

  def sendToKafka(data: String): Unit = {
    kafkaEmitter.send(new ProducerRecord(topic, data))
  }

  private def createKafkaEmitter(): Producer[String, String] = {
    new KafkaProducer(Map[String, Object](
      "bootstrap.servers" -> server,
      "acks" -> "all",
      "retries" -> "0",
      "linger.ms" -> "1",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    ).asJava)
  }
}
