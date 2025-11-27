package com.herminiogarcia.shexml.streaming

import com.herminiogarcia.shexml.helper.PicocliLeftAlignedLayout
import com.herminiogarcia.shexml.streaming.helpers.ObservablePrinter
import com.herminiogarcia.shexml.streaming.model.KafkaOptions
import monix.execution.Scheduler.Implicits.global
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

import java.time.Duration
import java.util.concurrent.Callable

object Main {

  def main(args: Array[String]): Unit = {
    val commandLine = new CommandLine(new Main())
      .setPosixClusteredShortOptionsAllowed(false)
      .setHelpFactory(PicocliLeftAlignedLayout.createCustomizedUsageHelp())
    System.exit(commandLine.execute(args: _*))
  }
}

@Command(name = "shexml-streaming", version = Array("v0.0.1"),
  mixinStandardHelpOptions = true,
  sortOptions = false,
  description = Array("Map and merge heterogeneous data sources using ShExML over streams"))
class Main extends Callable[Int] {

  @Option(names = Array("-m", "--mapping"), required = true, description = Array("Path to the file containing the mapping rules."))
  private var file: String = ""

  @Option(names = Array("-o", "--output"), description = Array("Path where the output file should be created. As a stream-based application the user is required to set the format to a RDF stream-compatible format or clean the output"))
  private var output: String = ""

  @Option(names = Array("-f", "--format"), description = Array("Output format for RDF graph. Turtle, RDF/XML, N-Triples, etc. Default value: N-Triples."))
  private var format: String = "N-Triples"

  @Option(names = Array("-id", "--inferenceDatatypes"), description = Array("Use the inference system for choosing the best suited datatype for the generated literal. Without this option, and not declaring a datatype in the mapping rules, all the literals will be outputted as strings"))
  private var inferenceDatatype: Boolean = false

  @Option(names = Array("-nu", "--normaliseURIs"), description = Array("Activate the URI normalisation system which allows to avoid malformed URIs when using strings for URI creation"))
  private var normaliseURIs: Boolean = false

  @Option(names = Array("--kafkaGroupId"), description = Array("GroupId to be used for consuming Kafka messages."))
  private var kafkaGroupId: String = ""

  @Option(names = Array("--kafkaPollTimeout"), description = Array("Timeout in ms to be used for polling Kafka messages."))
  private var kafkaPollTimeout: String = ""

  @Option(names = Array("--kafkaConsumeFromBeginning"), description = Array("Use this option to consume the topic from the beginning."))
  private var kafkaConsumeFromBeginning: Boolean = false

  override def call(): Int = {
    val fileHandler = if(file == "-") scala.io.Source.stdin else scala.io.Source.fromFile(file)
    try {
      val fileContent = fileHandler.mkString
      val groupId = if(kafkaGroupId.isEmpty) None else Some(kafkaGroupId)
      val pollTimeout = if(kafkaPollTimeout.isEmpty) None else Some(kafkaPollTimeout).map(_.toInt).map(Duration.ofMillis(_))
      val kafkaOptions = KafkaOptions(groupId, pollTimeout, kafkaConsumeFromBeginning)
      val streamMappingLauncher = new StreamMappingLauncher(
        inferenceDatatype = inferenceDatatype,
        normaliseURIs = normaliseURIs,
        kakfaOptions = kafkaOptions
      )
      val resultAsObservable = streamMappingLauncher.launchMapping(fileContent, format)
      resultAsObservable.flatMap(o => {
        val observablePrinter = new ObservablePrinter(o)
        if (output.isEmpty) observablePrinter.printToStout else observablePrinter.printToFile(output)
      }).runSyncUnsafe()
      0 // well finished
    } finally { fileHandler.close() }
  }
}
