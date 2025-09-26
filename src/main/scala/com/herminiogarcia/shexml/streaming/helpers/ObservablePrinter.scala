package com.herminiogarcia.shexml.streaming.helpers

import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.reactive.Observable

import java.io.{FileWriter, PrintWriter}

class ObservablePrinter[T](observable: Observable[T]) {

  private val logger = Logger[ObservablePrinter[T]]

  def printToFile(filepath: String): Task[Unit] = {
    observable.foreachL(r => {
      logger.info("Appending transformed event data to file")
      val pw = new PrintWriter(new FileWriter(filepath, true))
      pw.write(r.toString)
      pw.close()
    })
  }

  def printToStout: Task[Unit] = {
    observable.foreachL(println)
  }

}
