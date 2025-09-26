package com.herminiogarcia.shexml.streaming.helpers

import io.reactivex.rxjava3.core.Single
import monix.eval.Task
import monix.reactive.Observable
import monix.execution.Scheduler.Implicits.global
import scala.language.implicitConversions

object ReactiveConverters {

  object Implicits {
    implicit class TaskWithObservableConverter[T](taskWithObservable: Task[Observable[T]]) {
      def toRxJava: Single[io.reactivex.rxjava3.core.Observable[T]] = convertToRxJava(taskWithObservable)
    }
  }

  def convertToRxJava[T](taskWithObservable: Task[Observable[T]]): Single[io.reactivex.rxjava3.core.Observable[T]] = {
    val publisher = taskWithObservable.map(o => {
      val publisher = o.toReactivePublisher
      io.reactivex.rxjava3.core.Observable.fromPublisher(publisher)
    }).toReactivePublisher
    Single.fromPublisher(publisher)
  }

}


