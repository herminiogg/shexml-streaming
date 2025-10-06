package com.herminiogarcia.shexml.streaming

import com.herminiogarcia.shexml.MappingLauncher
import com.herminiogarcia.shexml.streaming.handlers.HandlerFactory
import com.herminiogarcia.shexml.streaming.model.StreamSource
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.reactive.Observable
import org.apache.jena.query.Dataset
import sttp.client4.UriContext
import collection.JavaConverters._
import java.io.ByteArrayInputStream

class StreamMappingLauncher(val inferenceDatatype: Boolean = false,
                            val normaliseURIs: Boolean = false) {

  private val logger = Logger[StreamMappingLauncher]

  private val shexmlMappingLauncher = new MappingLauncher(
    inferenceDatatype = inferenceDatatype,
    normaliseURIs = normaliseURIs
  )

  def launchMapping(mappingCode: String, lang: String): Task[Observable[String]] = {
    launchMapping(mappingCode, mr => shexmlMappingLauncher.launchMapping(mr, lang))
  }

  def launchMapping(mappingCode: String): Task[Observable[Dataset]] = {
    launchMapping(mappingCode, mr => shexmlMappingLauncher.launchMapping(mr))
  }

  private def launchMapping[T](mappingCode: String, shexmlGenerationMethod: String => T): Task[Observable[T]] = {
    logger.info(s"Launching mapping")
    logger.debug(s"Input mapping rules $mappingCode")
    parseStreamConfiguration(mappingCode) match {
      case Some(streamSource) =>
        val handler = HandlerFactory.createFromStreamSource(streamSource)
        val request = handler.request(uri"${streamSource.url}")
        request.map(c => {
          c.map(i => {
            launchStreamGeneration(mappingCode, streamSource, i, shexmlGenerationMethod)
          })
        })
      case None =>
        logger.warn("The provided mapping rules do not contain any STREAM source. Proceeding to launch the ShExML engine in a single event stream...")
        Task(Observable.delay(shexmlGenerationMethod(mappingCode)))
    }

  }

  private def parseStreamConfiguration(mappingRules: String): Option[StreamSource] = {
    mappingRules.lines().iterator().asScala.find(_.startsWith("STREAM")).map(l => StreamSource(l.trim.split("\\s+")))
  }

  private def launchStreamGeneration[T](mappingRules: String, streamSource: StreamSource, eventContent: String, shexmlGenerationMethod: String => T): T = {
    val intermediateShExML = mappingRules.replaceFirst("STREAM.+>", s"SOURCE ${streamSource.name} <stdin>")
    val precompiledShExML = shexmlMappingLauncher.precompile(intermediateShExML)
    logger.info("Launching the ShExML engine to convert the received event")
    logger.debug(s"Mapping rules for event processing: $precompiledShExML")
    val inputStream = new ByteArrayInputStream(eventContent.getBytes("UTF-8"))
    shexmlMappingLauncher.getClass.synchronized {
      System.setIn(inputStream)
      inputStream.close()
      val result = shexmlGenerationMethod(precompiledShExML)
      System.setIn(System.in)
      result
    }
  }
}
