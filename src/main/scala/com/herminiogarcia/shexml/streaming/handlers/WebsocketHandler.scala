package com.herminiogarcia.shexml.streaming.handlers

import cats.effect.ExitCase
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.reactive.{Observable, OverflowStrategy}
import sttp.capabilities.monix.MonixStreams.Pipe
import sttp.client4.httpclient.monix.HttpClientMonixBackend
import sttp.model.Uri
import sttp.ws.WebSocketFrame
import sttp.client4._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.observers.Subscriber
import sttp.capabilities.monix.MonixStreams
import sttp.client4.impl.monix.MonixWebSockets
import sttp.client4.ws.stream.asWebSocketStream

class WebsocketHandler extends Handler {

  private val logger = Logger[WebsocketHandler]


  def request(uri: Uri): Task[Observable[String]] = Task {
    MonixWebSockets.combinedTextFrames(Observable.create[WebSocketFrame](OverflowStrategy.Unbounded) { sub =>
      val response = HttpClientMonixBackend.resource().use { backend =>
        logger.info("Connecting to Websocket...")
        logger.debug(s"Connecting to Websocket: $uri")
        basicRequest
          .get(uri)
          .response(asWebSocketStream(MonixStreams)(webSocketFramePipe(sub)))
          .send(backend).map(_.body match {
            case Left(error) =>
              logger.error(s"Error received while connecting to the websocket: $error")
              sub.onError(new Exception(s"Error while retrieving the websocket data: $error"))
            case Right(value) => value
          }).guarantee(backend.close())
      }
      response.guarantee(Task(sub.onComplete())).runToFuture(sub.scheduler)
    })
  }

  private def webSocketFramePipe(subscriber: Subscriber[WebSocketFrame]): Pipe[WebSocketFrame.Data[_], WebSocketFrame] = input => {
    input.map {
      case t: WebSocketFrame.Text =>
        subscriber.onNext(t)
        t
      case WebSocketFrame.Binary(data, ff, rsv) =>
        val wsf = WebSocketFrame.Text(new String(data), ff, rsv)
        subscriber.onNext(wsf)
        wsf
    }.guaranteeCase {
      case ExitCase.Error(error) => Task(logger.error(s"Error while processing the websocket stream $error."))
      case ExitCase.Canceled => Task(logger.error(s"The websocket stream was canceled or interrupted."))
      case ExitCase.Completed => Task(logger.info(s"The websocket stream was closed by origin and it has been completely processed."))
    }
  }

}
