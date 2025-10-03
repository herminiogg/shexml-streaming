package com.herminiogarcia.shexml.streaming

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.scalatest.funsuite.AnyFunSuite

class SSECsvTest extends AnyFunSuite with RDFStatementCreator with ResultRetriever {

  private val prefix = "http://example.com/"
  private val output = retrieveResult("src/test/resources/sseFilmsCsv.shexml", normaliseURIs = true, inferDatatypes = true)

  test("Result contains film 1") {
    assert(output.contains(createStatement(prefix, "1", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "1", "name", "film_1", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "1", "year", "1951", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "1", "event_id", "1", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 2") {
    assert(output.contains(createStatement(prefix, "2", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "2", "name", "film_2", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "2", "year", "1952", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "2", "event_id", "2", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 3") {
    assert(output.contains(createStatement(prefix, "3", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "3", "name", "film_3", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "3", "year", "1953", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "3", "event_id", "3", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 4") {
    assert(output.contains(createStatement(prefix, "4", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "4", "name", "film_4", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "4", "year", "1954", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "4", "event_id", "4", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 5") {
    assert(output.contains(createStatement(prefix, "5", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "5", "name", "film_5", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "5", "year", "1955", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "5", "event_id", "5", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 6") {
    assert(output.contains(createStatement(prefix, "6", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "6", "name", "film_6", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "6", "year", "1956", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "6", "event_id", "6", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 7") {
    assert(output.contains(createStatement(prefix, "7", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "7", "name", "film_7", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "7", "year", "1957", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "7", "event_id", "7", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 8") {
    assert(output.contains(createStatement(prefix, "8", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "8", "name", "film_8", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "8", "year", "1958", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "8", "event_id", "8", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 9") {
    assert(output.contains(createStatement(prefix, "9", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "9", "name", "film_9", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "9", "year", "1959", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "9", "event_id", "9", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 10") {
    assert(output.contains(createStatement(prefix, "10", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "10", "name", "film_10", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "10", "year", "1951", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "10", "event_id", "1", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 11") {
    assert(output.contains(createStatement(prefix, "11", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "11", "name", "film_11", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "11", "year", "1952", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "11", "event_id", "2", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 12") {
    assert(output.contains(createStatement(prefix, "12", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "12", "name", "film_12", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "12", "year", "1953", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "12", "event_id", "3", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 13") {
    assert(output.contains(createStatement(prefix, "13", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "13", "name", "film_13", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "13", "year", "1954", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "13", "event_id", "4", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 14") {
    assert(output.contains(createStatement(prefix, "14", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "14", "name", "film_14", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "14", "year", "1955", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "14", "event_id", "5", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 15") {
    assert(output.contains(createStatement(prefix, "15", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "15", "name", "film_15", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "15", "year", "1956", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "15", "event_id", "6", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 16") {
    assert(output.contains(createStatement(prefix, "16", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "16", "name", "film_16", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "16", "year", "1957", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "16", "event_id", "7", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 17") {
    assert(output.contains(createStatement(prefix, "17", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "17", "name", "film_17", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "17", "year", "1958", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "17", "event_id", "8", XSDDatatype.XSDinteger)))
  }

  test("Result contains film 18") {
    assert(output.contains(createStatement(prefix, "18", "type", "Film")))
    assert(output.contains(createStatementWithLiteral(prefix, "18", "name", "film_18", XSDDatatype.XSDstring)))
    assert(output.contains(createStatementWithLiteral(prefix, "18", "year", "1959", XSDDatatype.XSDinteger)))
    assert(output.contains(createStatementWithLiteral(prefix, "18", "event_id", "9", XSDDatatype.XSDinteger)))
  }

}
