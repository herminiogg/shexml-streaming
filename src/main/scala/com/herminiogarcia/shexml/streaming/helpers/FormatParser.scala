package com.herminiogarcia.shexml.streaming.helpers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.w3c.dom.Document

import java.io.{ByteArrayInputStream, StringWriter}
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

object FormatParser {

  object Implicits {
    implicit class StringToFormat(input: String) {
      def toJson: JsonNode = asJson(input)
      def toXml: Document = asXml(input)
    }

    implicit class JsonNodeAsString(input: JsonNode) {
      def asString: String = jsonToString(input)
    }

    implicit class XmlDocumentAsString(input: Document) {
      def asString: String = xmlToString(input)
    }
  }

  def asJson(input: String): JsonNode = new ObjectMapper().readTree(input)

  def asXml(input: String): Document = {
    val factory = DocumentBuilderFactory.newInstance()
    val docBuilder = factory.newDocumentBuilder()
    docBuilder.setErrorHandler(new XMLParserErrorHandler())
    val inputStream = new ByteArrayInputStream(input.getBytes())
    val document = docBuilder.parse(inputStream)
    inputStream.close()
    document
  }

  def jsonToString(input: JsonNode): String = {
    input.toString
  }

  def xmlToString(input: Document): String = {
    val tf = TransformerFactory.newInstance()
    val transformer = tf.newTransformer()
    val writer = new StringWriter()
    transformer.transform(new DOMSource(input), new StreamResult(writer));
    val stringRepresentation = writer.getBuffer.toString
    writer.close()
    stringRepresentation
  }

}
