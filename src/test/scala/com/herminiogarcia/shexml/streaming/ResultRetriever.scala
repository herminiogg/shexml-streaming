package com.herminiogarcia.shexml.streaming

import com.herminiogarcia.shexml.streaming.model.KafkaOptions
import monix.eval.Task
import org.apache.jena.rdf.model.Model
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.jena.query.{Dataset, DatasetFactory}

import java.io.File

trait ResultRetriever {

  def retrieveResult(filepath: String, normaliseURIs: Boolean = false, inferDatatypes: Boolean = false): Model = {
    retrieveResultAsync(filepath, normaliseURIs, inferDatatypes).runSyncUnsafe()
  }

  def retrieveNResults(n: Int, filepath: String, normaliseURIs: Boolean = false, inferDatatypes: Boolean = false): Model = {
    retrieveNResultsAsync(n, filepath, normaliseURIs, inferDatatypes).runSyncUnsafe()
  }

  def retrieveResultAsync(filepath: String, normaliseURIs: Boolean = false, inferDatatypes: Boolean = false): Task[Model] = {
    val dataset = DatasetFactory.create()
    getResultAsObservable(filepath, normaliseURIs, inferDatatypes)
      .flatMap(_.foldLeftL(dataset.getDefaultModel)((a, b) => a.add(b.getDefaultModel)))
      .map(_ => dataset.getDefaultModel)
  }

  def retrieveNResultsAsync(n: Int, filepath: String, normaliseURIs: Boolean = false, inferDatatypes: Boolean = false, streamLineModification: Option[String] = None): Task[Model] = {
    val dataset = DatasetFactory.create()
    getResultAsObservable(filepath, normaliseURIs, inferDatatypes, streamLineModification)
      .map(_.take(n))
      .flatMap(_.foldLeftL(dataset.getDefaultModel)((a, b) => a.add(b.getDefaultModel)))
      .map(_ => dataset.getDefaultModel)
  }

  private def getResultAsObservable(filepath: String, normaliseURIs: Boolean = false, inferDatatypes: Boolean = false, streamLineModification: Option[String] = None): Task[Observable[Dataset]] = {
    val source = scala.io.Source.fromFile(new File(filepath))
    val mappingRules = streamLineModification.map(source.mkString.replaceFirst("STREAM.*", _)).getOrElse(source.mkString)
    source.close()
    val kafkaOptions = KafkaOptions(None, None, consumeFromBeginning = true)
    new StreamMappingLauncher(inferenceDatatype = inferDatatypes, normaliseURIs = normaliseURIs, kakfaOptions = kafkaOptions)
      .launchMapping(mappingRules)
  }

}
