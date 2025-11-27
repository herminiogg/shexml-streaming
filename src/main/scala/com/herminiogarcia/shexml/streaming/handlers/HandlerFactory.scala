package com.herminiogarcia.shexml.streaming.handlers

import com.herminiogarcia.shexml.streaming.model.{KafkaOptions, KafkaSource, SSESource, StreamSource, WebsocketSource}
import monix.eval.Task
import monix.reactive.Observable
import sttp.client4.UriContext
import java.time.Duration

trait Handler {
  def request: Task[Observable[String]]
}

object HandlerFactory {

  def createFromStreamSource(streamSource: StreamSource, kafkaOptions: KafkaOptions): Handler = streamSource match {
    case WebsocketSource(_, url) => new WebsocketHandler(uri"$url")
    case SSESource(_, url) => new SSEHandler(uri"$url")
    case KafkaSource(_, server, topic) =>
      new KafkaHandler(
        server,
        topic,
        kafkaOptions.groupId.getOrElse("shexml-streaming"),
        kafkaOptions.pollTimeout.getOrElse(Duration.ofMillis(1000)),
        kafkaOptions.consumeFromBeginning
      )
  }

}
