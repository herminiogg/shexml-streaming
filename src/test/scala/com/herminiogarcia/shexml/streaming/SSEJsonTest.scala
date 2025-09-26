package com.herminiogarcia.shexml.streaming

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.scalatest.funsuite.AnyFunSuite

class SSEJsonTest extends AnyFunSuite with RDFStatementCreator with ResultRetriever {

  private val prefix = "http://example.com/"
  private val output = retrieveResult("src/test/resources/sseFilmsJson.shexml")

  test("Result contains film 1") {
    assert(output.contains(createStatement(prefix, "1", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "1", "name", "film_1", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "1", "year", "1951", XSDDatatype.XSDstring)))
  }

  test("Result contains film 2") {
    assert(output.contains(createStatement(prefix, "2", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "2", "name", "film_2", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "2", "year", "1952", XSDDatatype.XSDstring)))
  }

  test("Result contains film 3") {
    assert(output.contains(createStatement(prefix, "3", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "3", "name", "film_3", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "3", "year", "1953", XSDDatatype.XSDstring)))
  }

  test("Result contains film 4") {
    assert(output.contains(createStatement(prefix, "4", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "4", "name", "film_4", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "4", "year", "1954", XSDDatatype.XSDstring)))
  }

  test("Result contains film 5") {
    assert(output.contains(createStatement(prefix, "5", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "5", "name", "film_5", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "5", "year", "1955", XSDDatatype.XSDstring)))
  }

  test("Result contains film 6") {
    assert(output.contains(createStatement(prefix, "6", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "6", "name", "film_6", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "6", "year", "1956", XSDDatatype.XSDstring)))
  }

  test("Result contains film 7") {
    assert(output.contains(createStatement(prefix, "7", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "7", "name", "film_7", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "7", "year", "1957", XSDDatatype.XSDstring)))
  }

  test("Result contains film 8") {
    assert(output.contains(createStatement(prefix, "8", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "8", "name", "film_8", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "8", "year", "1958", XSDDatatype.XSDstring)))
  }

  test("Result contains film 9") {
    assert(output.contains(createStatement(prefix, "9", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "9", "name", "film_9", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "9", "year", "1959", XSDDatatype.XSDstring)))
  }

}
