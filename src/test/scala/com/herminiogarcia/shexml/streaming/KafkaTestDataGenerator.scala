package com.herminiogarcia.shexml.streaming

trait KafkaTestDataGenerator {

  private def xmlTemplate(i: Int) = s"<films><film><id>$i</id><name>film_$i</name><year>${1950 + i}</year></film></films>"

  def generateFilmsDataJson: IndexedSeq[String] = {
    for(i <- 1 to 10) yield {
      s"{'id': $i, 'name': 'film_$i', 'year': ${1950 + i}}"
    }
  }

  def generateFilmsDataXmlSet1: IndexedSeq[String] = {
    for(i <- 1 to 5) yield {
      xmlTemplate(i)
    }
  }

  def generateFilmsDataXmlSet2: IndexedSeq[String] = {
    for(i <- 6 to 10) yield {
      xmlTemplate(i)
    }
  }

}
