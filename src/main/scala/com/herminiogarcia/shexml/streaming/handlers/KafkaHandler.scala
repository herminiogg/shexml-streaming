package com.herminiogarcia.shexml.streaming.handlers

import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.reactive.{Observable, OverflowStrategy}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import scala.collection.JavaConverters._

class KafkaHandler(val servers: List[String],
                   val topics: List[String],
                   val groupId: String,
                   val pollTimeout: Duration,
                   val consumeFromBeginning: Boolean,
                  ) extends Handler {

  private val logger = Logger[KafkaHandler]

  def request: Task[Observable[String]] = Task {
    Observable.create[String](OverflowStrategy.Unbounded) { sub =>
      logger.info("Connecting to Kafka...")
      logger.debug(s"Details for Kafka connection: servers $servers, topics $topics groupId $groupId, pollTimeout $pollTimeout, consumeFromBeginning $consumeFromBeginning")
      val fromBeginningOption = if(consumeFromBeginning) Map("auto.offset.reset" -> "earliest") else Map()
      val kafkaConsumer = new KafkaConsumer[String, String]((Map[String, Object](
        "bootstrap.servers" -> servers.mkString(","),
        "group.id"-> groupId,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
      ) ++ fromBeginningOption).asJava)
      kafkaConsumer.subscribe(topics.asJava)
      Task {
        while(true) {
          val messages = kafkaConsumer.poll(pollTimeout)
          messages.forEach(m => sub.onNext(m.value()))
        }
      }.guarantee(Task {
        logger.info("Kafka message consumption finished, unsubscribing and closing connection...")
        sub.onComplete()
        kafkaConsumer.unsubscribe()
        kafkaConsumer.close()
      }).runToFuture(sub.scheduler)
    }
  }
}