package com.herminiogarcia.shexml.streaming.model

case class StreamSource(name: String, url: String)

object StreamSource {
  def apply(tokens: Array[String]): StreamSource = {
    new StreamSource(tokens(1), tokens(2).replaceAll("<", "").replaceAll(">", ""))
  }
}
