package com.herminiogarcia.shexml.streaming.model

import scala.util.parsing.combinator._

sealed trait StreamSource {
  val name: String
}
case class WebsocketSource(name: String, url: String) extends StreamSource
case class SSESource(name: String, url: String) extends StreamSource
case class KafkaSource(name: String, servers: List[String], topics: List[String]) extends StreamSource

object StreamSource extends StreamSourceParser {
  def apply(streamSourceLine: String): StreamSource = {
    parseAll(root, streamSourceLine) match {
      case Success(result, _) => result
      case Failure(msg, _) => throw new Exception(s"Parsing failure: $msg")
      case Error(msg, _) => throw new Exception(s"Parsing error: $msg")
    }
  }
}

class StreamSourceParser extends JavaTokenParsers {

  def root: Parser[StreamSource] =
    "STREAM" ~ name ~ "<" ~ (websocketUrl | sseUrl | kafkaDetails) ~ ">" ^^
      { case _ ~ name ~ _ ~ streamSource ~ _  => streamSource match {
        case WebsocketSource(_, url) => WebsocketSource(name, url)
        case SSESource(_, url) => SSESource(name, url)
        case KafkaSource(_, servers, topics) => KafkaSource(name, servers, topics)
      } }

  private def name: Parser[String] =
    letter ~ rep(letter | digit | allowedCharacters) ^^
      { case firstChar ~ listTailChars => firstChar + listTailChars.mkString }

  private def websocketUrl: Parser[WebsocketSource] =
    "wss?://".r ~ rep(letter | digit | allowedCharacters) ^^
      { case protocol ~ restOfUrl => WebsocketSource("", protocol + restOfUrl.mkString) }

  private def sseUrl: Parser[SSESource] = "https?://".r ~ rep(letter | digit | allowedCharacters) ^^
    { case protocol ~ restOfUrl => SSESource("", protocol + restOfUrl.mkString) }

  private def kafkaDetails: Parser[KafkaSource] = {
    "kafka:".r ~ rep1sep(rep(letter | digit | allowedCharacters), ",") ~
      "->".r ~ rep1sep(rep1(letter | digit | allowedCharacters), ",") ^^
    { case _ ~ kafkaServers ~ _ ~ kafkaTopics => KafkaSource("", kafkaServers.map(_.mkString), kafkaTopics.map(_.mkString))}
  }

  private def letter: Parser[String] = "[a-zA-Z]".r ^^ { _.toString }
  private def digit: Parser[String] = "[0-9]".r ^^ { _.toString }
  private def allowedCharacters: Parser[String] = "[$_.+!*'();/?:@=&%#]".r ^^ { _.toString }

}
