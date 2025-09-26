package com.herminiogarcia.shexml.streaming.helpers

import org.xml.sax.{ErrorHandler, SAXParseException}

class XMLParserErrorHandler extends ErrorHandler {

  override def warning(exception: SAXParseException): Unit = throw exception

  override def error(exception: SAXParseException): Unit = throw exception

  override def fatalError(exception: SAXParseException): Unit = throw exception
}
