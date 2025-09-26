package com.herminiogarcia.shexml.streaming.handlers

import cats.effect.ExitCase
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable, OverflowStrategy}
import sttp.capabilities.monix.MonixStreams
import sttp.capabilities.monix.MonixStreams.BinaryStream
import sttp.client4._
import sttp.client4.httpclient.monix.HttpClientMonixBackend
import sttp.client4.impl.monix.MonixServerSentEvents
import sttp.model.Uri
import sttp.model.sse.ServerSentEvent
import com.herminiogarcia.shexml.streaming.helpers.FormatParser.Implicits._
import scala.util.{Failure, Success, Try}

class SSEHandler extends Handler {

  private val logger = Logger[SSEHandler]

  def request(uri: Uri): Task[Observable[String]] = Task {
    Observable.create[String](OverflowStrategy.Unbounded) { sub =>
      val response = HttpClientMonixBackend.resource().use { backend =>
        logger.info("Connecting to SSE stream...")
        logger.debug(s"Connecting to SSE stream: $uri")
        basicRequest
          .get(uri)
          .response(asStream(MonixStreams)(parseStream(sub)))
          .send(backend).map(_.body match {
            case Right(obs) => obs
            case Left(error) =>
              logger.error(s"Error received while connecting to the SSE stream: $error")
              Observable.raiseError(new Exception(s"Error while retrieving the SSE stream: $error"))
          }).guarantee(backend.close())
      }
      response.guarantee(Task(sub.onComplete())).runToFuture(sub.scheduler)
    }
  }

  private def parseStream(sub: Subscriber[String]): BinaryStream => Task[Unit] = s => {
    s.transform(MonixServerSentEvents.parse).foreachL(i => {
      logger.info("Event received from the SSE stream")
      logger.debug(s"Event information: $i")
      sub.onNext(convertToDataFormat(i))
    }).guaranteeCase {
      case ExitCase.Error(error) => Task(logger.error(s"Error while processing the SSE stream $error."))
      case ExitCase.Canceled => Task(logger.error(s"The SSE stream was canceled or interrupted."))
      case ExitCase.Completed => Task(logger.info(s"The SSE stream was closed by origin and it has been completely processed."))
    }
  }

  private def convertToDataFormat(sse: ServerSentEvent): String = {
    Try(convertToJsonDocument(sse)) match {
      case Success(value) =>
        logger.debug("Event processed as a JSON document")
        value
      case Failure(_) => Try(convertToXmlDocument(sse)) match {
        case Success(value) =>
          logger.debug("Event processed as an XML document")
          value
        case Failure(_) =>
          logger.debug("Event converted to CSV")
          convertToCsvFormat(sse)
      }
    }
  }

  private def convertToJsonDocument(sse: ServerSentEvent): String = {
    val jsonNode = sse.data.getOrElse("").toJson
    val finalJsonNode = new ObjectMapper()
      .createObjectNode()
      .put("id", sse.id.getOrElse(""))
      .put("event", sse.eventType.getOrElse(""))
      .put("retry", sse.retry.getOrElse(-1))
    finalJsonNode.set("data", jsonNode)
    finalJsonNode.asString
  }

  private def convertToXmlDocument(sse: ServerSentEvent): String = {
    val parsedDocument = sse.data.getOrElse("").toXml
    parsedDocument.getDocumentElement.setAttribute("id", sse.id.getOrElse(""))
    parsedDocument.getDocumentElement.setAttribute("event", sse.eventType.getOrElse(""))
    parsedDocument.getDocumentElement.setAttribute("retry", sse.retry.map(_.toString).getOrElse("-1"))
    parsedDocument.asString
  }

  private def convertToCsvFormat(sse: ServerSentEvent): String = {
    "id;event;retry;data\n" +
      s"${sse.id.getOrElse("")};${sse.eventType.getOrElse("")};${sse.retry.getOrElse("")};${sse.data.getOrElse("")}"
  }
}