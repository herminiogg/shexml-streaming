package com.herminiogarcia.shexml.streaming.handlers

import com.herminiogarcia.shexml.streaming.model.StreamSource
import monix.eval.Task
import monix.reactive.Observable
import sttp.model.Uri

trait Handler {
  def request(uri: Uri): Task[Observable[String]]
}

object HandlerFactory {

  def createFromStreamSource(streamSource: StreamSource): Handler = {
    if(streamSource.url.startsWith("ws")) new WebsocketHandler
    else new SSEHandler
  }

}
