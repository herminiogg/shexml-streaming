package com.herminiogarcia.shexml.streaming.helpers

import io.reactivex.rxjava3.core.Single
import monix.eval.Task
import monix.reactive.Observable
import monix.execution.Scheduler.Implicits.global
import org.reactivestreams.Publisher
import scala.language.implicitConversions

object ReactiveConverters {

  object Implicits {
    implicit class TaskWithObservableConverter[T](taskWithObservable: Task[Observable[T]]) {
      def toRxJava: Single[io.reactivex.rxjava3.core.Observable[T]] = convertToRxJava(taskWithObservable)
    }

    implicit class TaskWithObservableToFlowableConverter[T](taskWithObservable: Task[Observable[T]]) {
      def toRxJavaFlowable: Single[io.reactivex.rxjava3.core.Flowable[T]] = convertToRxJavaFlowable(taskWithObservable)
    }
  }

  def convertToRxJava[T](taskWithObservable: Task[Observable[T]]): Single[io.reactivex.rxjava3.core.Observable[T]] = {
    singleToRxJava(taskWithObservable, io.reactivex.rxjava3.core.Observable.fromPublisher(_))
  }

  def convertToRxJavaFlowable[T](taskWithObservable: Task[Observable[T]]): Single[io.reactivex.rxjava3.core.Flowable[T]] = {
    singleToRxJava(taskWithObservable, io.reactivex.rxjava3.core.Flowable.fromPublisher(_))
  }

  private def singleToRxJava[T, R[_]](taskWithObservable: Task[Observable[T]], publisherConverter: Publisher[T] => R[T]): Single[R[T]]  = {
    val publisher = taskWithObservable.map(o => {
      val publisher = o.toReactivePublisher
      publisherConverter(publisher)
    }).toReactivePublisher
    Single.fromPublisher(publisher)
  }

}


