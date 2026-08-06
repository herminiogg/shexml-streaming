package com.herminiogarcia.shexml.streaming.model

import java.time.Duration

case class KafkaOptions(groupId: Option[String], pollTimeout: Option[Duration], consumeFromBeginning: Boolean)
