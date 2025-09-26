package com.herminiogarcia.shexml.streaming

import org.apache.jena.rdf.model.Model
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.DatasetFactory

import java.io.File

trait ResultRetriever {

  def retrieveResult(filepath: String): Model = {
    val source = scala.io.Source.fromFile(new File(filepath))
    val mappingRules = source.mkString
    source.close()
    val dataset = DatasetFactory.create()
    new StreamMappingLauncher()
      .launchMapping(mappingRules)
      .flatMap(_.foldLeftL(dataset.getDefaultModel)((a, b) => a.add(b.getDefaultModel)))
      .runSyncUnsafe()
    dataset.getDefaultModel
  }

}
