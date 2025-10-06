package com.herminiogarcia.shexml.streaming.handlers

import cats.effect.ExitCase
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.tototoshi.csv.{CSVParser, CSVReader, DefaultCSVFormat}
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

import java.io.StringReader
import javax.xml.parsers.DocumentBuilderFactory
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
        case Failure(_) => Try(convertToCsvFormat(sse)) match {
          case Success(value) =>
            logger.debug("Event processed as CSV document")
            value
          case Failure(_) =>
            logger.debug("SSE event converted to CSV")
            textToCsv(sse)
        }

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
    val factory = DocumentBuilderFactory.newInstance()
    val document = factory.newDocumentBuilder().newDocument()
    val rootElement = document.createElement("sse")
    document.appendChild(rootElement)
    rootElement.appendChild(document.createElement("id")).setTextContent(sse.id.getOrElse(""))
    rootElement.appendChild(document.createElement("event")).setTextContent(sse.eventType.getOrElse(""))
    rootElement.appendChild(document.createElement("retry")).setTextContent(sse.retry.map(_.toString).getOrElse("-1"))
    rootElement.appendChild(document.createElement("data"))
      .appendChild(document.importNode(sse.data.getOrElse("").toXml.getDocumentElement, true))
    document.asString
  }

  private def convertToCsvFormat(sse: ServerSentEvent): String = {
    val id = sse.id.getOrElse("")
    val event = sse.eventType.getOrElse("")
    val retry = sse.retry.getOrElse(-1).toString
    val data = sse.data.get
    val fileDelimiter = inferCSVDelimiter(data)
    implicit object MyCSVFormat extends DefaultCSVFormat {
      override val delimiter = fileDelimiter
    }
    val allLines = CSVReader.open(new StringReader(data)).all()
    if(!(allLines.size >= 2 && allLines.head.size - 1 == data.linesIterator.next().count(_ == fileDelimiter))) {
      throw new Exception("Not in CSV format")
    }
    val headers = List("id", "event", "retry") ::: allLines.head.map("data_" + _)
    val content = allLines.tail.map(List(id, event, retry) ::: _)
    (headers :: content map { i =>
      i.mkString(fileDelimiter.toString)
    }).mkString("\n")
  }

  private def textToCsv(sse: ServerSentEvent): String = {
    "id;event;retry;data\n" +
      s"${sse.id.getOrElse("")};${sse.eventType.getOrElse("")};${sse.retry.getOrElse("")};${sse.data.getOrElse("")}"
  }

  private def inferCSVDelimiter(fileContent: String): Char = {
    val comma = fileContent.count(_.equals(','))
    val semicolon = fileContent.count(_.equals(';'))
    val dot = fileContent.count(_.equals('.'))
    val colon = fileContent.count(_.equals(':'))
    val at = fileContent.count(_.equals('@'))
    val sharp = fileContent.count(_.equals('#'))
    val tab = fileContent.count(_.equals('\t'))
    val map = Map(',' -> comma, ';' -> semicolon, '.' -> dot, ':' -> colon, '@' -> at, '#' -> sharp, '\t' -> tab)
    val result = map.foldLeft(',')((greater, count) => if(map(greater) < count._2) count._1 else greater)
    logger.debug(s"Inferred CSV delimiter is: $result")
    result
  }
}