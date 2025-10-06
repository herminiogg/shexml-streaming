# shexml-streaming
[![Scala CI](https://github.com/herminiogg/shexml-streaming/actions/workflows/scala.yml/badge.svg)](https://github.com/herminiogg/shexml-streaming/actions/workflows/scala.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.herminiogarcia/shexml-streaming_3?color=blue)](https://central.sonatype.com/artifact/com.herminiogarcia/shexml-streaming_3)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.17278967.svg)](https://doi.org/10.5281/zenodo.17278967)
[![SWH](https://archive.softwareheritage.org/badge/origin/https://github.com/herminiogg/shexml-streaming/)](https://archive.softwareheritage.org/browse/origin/?origin_url=https://github.com/herminiogg/shexml-streaming)

This is a wrapper over the [ShExML engine](https://github.com/herminiogg/ShExML) allowing to
consume asynchronous data streams in a non-blocking fashion. It introduces the `STREAM` declaration which
is leveraged to produce asynchronous calls to the ShExML engine upon the received events. The implementation is
heavily based on the Reactive Programming precepts delegating the actual execution and processing to the final client.

## Features
* Server-Sent Events (SSE) support
  * Inclusion of id, event and retry in the input data
  * Formats:
    * JSON, XML and CSV
    * Free text on data (internally converted to a CSV input)
* Websockets support
  * Any format supported by ShExML
  * Unidirectional (only receiving messages)

## Implementation details
This library relies on [sttp](https://sttp.softwaremill.com/en/latest/) for the consumption of SSEs and Websockets and their
conversion to streams. At the same time, Monix is used to represent those streams as an [Observable](https://monix.io/docs/current/reactive/observable.html)
which can then be transformed and passed on as a non-blocking asynchronous events iterator. In practice, this means that
this library will construct an Observable for you embedding the consumption of the indicated stream
and apply certain transformations to it, however the computation will be delayed until you decide to consume it. Additionally,
the observable will be embedded in a [Task](https://monix.io/docs/current/eval/task.html) type which ensures that even
the computation needed to create the Observable is not executed before the final user decides so.
In the example below you can see how the `StreamMappingLauncher` can be called, the returned `Task[Observable[String]]`
consumed and its results printed to the standard output.

```scala
val obs = new StreamMappingLauncher().launchMapping(mappingRules, "N-Triples")
obs.flatMap(_.foreachL(println)).runSyncUnsafe()
```

### Monix and RxJava
Given that Monix is meant to be a very idiomatic solution for Scala environments, it can present additional difficulties 
when used from Java - or JVM - applications. However, due to Monix good compatibility with the Reactive Streams specification
it is fairly straightforward to convert this library return types to a more Java-based one. To this effect, this library
incorporates a `ReactiveConverters` object that can be used to convert the results from Monix to RxJava. The following example
shows how to call this library from a Java application.

```java
Single<Observable<String>> obs = ReactiveConverters.convertToRxJava(
    new StreamMappingLauncher(true, true)
        .launchMapping(mappingRules, "N-Triples"));
obs.blockingGet().blockingForEach(System.out::println);
```

At the same time, it is also possible to use the RxJava Observable from Scala and run the conversion using implicits.
```scala
import com.herminiogarcia.shexml.streaming.helpers.ReactiveConverters.Implicits._

val obs = new StreamMappingLauncher().launchMapping(mappingRules, "N-Triples").toRxJava
obs.blockingGet().blockingForEach(println)
```

### Websockets
Websockets are treated in shexml-streaming as a unidirectional source, in other words, you cannot send any message to the
websocket but can consume events as long as the stream is not closed. Below, you can see an example of consuming films
from a websocket alongside the input and result of processing that event.

```
PREFIX : <http://example.com/>
STREAM films_stream <ws://localhost:8004>
ITERATOR film_json <jsonpath: $> {
  FIELD id <id>
  FIELD name <name>
  FIELD year <year>
}

EXPRESSION films <films_stream.film_json>

:Films :[films.id] {
  :type :Film ;
  :name [films.name] ;
  :year [films.year] ;
}
```

#### Input
```json
{"id":1,"name":"film_1","year":1951}
```
#### Output
```
<http://example.com/1> <http://example.com/type> <http://example.com/Film> .
<http://example.com/1> <http://example.com/name> "film_1" .
<http://example.com/1> <http://example.com/year> "1951" .
```

### SSE
Server-Sent Events are unidirectional by nature so only consumption of events is expected from a client. 
At the same time, the specification already imposes a specific format for the events, including optional id, event 
and retry attributes and a mandatory data one. Given that ShExML does not natively support querying this format, 
shexml-streaming preprocesses the event, includes the optional fields values under the transmitted data and transfers the 
final structured format to ShExML. Right now, it allows transforming XML and JSON inputs, and when provided with plain text
it will transform the event to a CSV input. The example below shows how an event containing data in the JSON format is 
transformed. You can see more examples in the [tests folder](src/test/resources).

```
PREFIX : <http://example.com/>
STREAM films_stream <http://localhost:8002/films>
ITERATOR film_json <jsonpath: $> {
  FIELD id <id>
  FIELD name <data.name>
  FIELD year <data.year>
}

EXPRESSION films <films_stream.film_json>

:Films :[films.id] {
  :type :Film ;
  :name [films.name] ;
  :year [films.year] ;
}
```

#### Input JSON
```
event: new_item
id: 1
data: {"name": "film_1", "year": 1951}
```
#### Output for JSON input
```
<http://example.com/1> <http://example.com/type> <http://example.com/Film> .
<http://example.com/1> <http://example.com/name> "film_1" .
<http://example.com/1> <http://example.com/year> "1951" .
```

## CLI
This library can also be executed from the CLI following the reference included below. Unlike the JVM API which is intended
for further reuse by other applications, the CLI will automatically subscribe to the received Observable and execute it. 

```
Usage: shexml-streaming [-h] [-id] [-nu] [-V] [-f=<format>] -m=<file>
                        [-o=<output>]
Map and merge heterogeneous data sources using ShExML over streams
  -m, --mapping=<file>         Path to the file containing the mapping rules.
  -o, --output=<output>        Path where the output file should be created. As
                                 a stream-based application the user is
                                 required to set the format to a RDF
                                 stream-compatible format or clean the output
  -f, --format=<format>        Output format for RDF graph. Turtle, RDF/XML,
                                 N-Triples, etc. Default value: N-Triples.
  -id, --inferenceDatatypes    Use the inference system for choosing the best
                                 suited datatype for the generated literal.
                                 Without this option, and not declaring a
                                 datatype in the mapping rules, all the
                                 literals will be outputted as strings
  -nu, --normaliseURIs         Activate the URI normalisation system which
                                 allows to avoid malformed URIs when using
                                 strings for URI creation
  -h, --help                   Show this help message and exit.
  -V, --version                Print version information and exit.
```

## Requirements
The minimal versions for this software to work are:
- JDK 17, or the Open JDK 17. (Versions matching earlier JDK versions can be generated following the [Build](#build) instructions or provided upon request.)
- Scala 2.12.20
- SBT 1.11.6

## Build
The library uses sbt as the package manager and building tool, therefore to compile the project you can use the following command:
```
$ sbt compile
```
To run the project from within sbt you can use the command below, where `<options>` can be replaced by the arguments explained in the [CLI](#cli).
```
$ sbt "run <options>"
```
To generate an executable JAR file you can call the following command. Take into account that if you want to test the library before
generating the artifact you need to set up the testing environment as explained in the [Testing](#testing) section. Alternatively, you can use
the `"set test in assembly := {}"` option to omit the tests during the build process.
```
$ sbt "set test in assembly := {}" clean update assembly
```

## Testing
The project contains a full suite of tests that checks that all the features included in the library work as expected. These
tests units are included under the src/test/scala folder. To run them you can use the command below. Notice that it is of utmost
importance to test that the project pass the test for all the cross-compiled versions used within the project
(see the [Cross-compilation](#cross-compilation) section for more details).
```
$ sbt test
```
The test environment uses some external resources that need to be set up before running them. This mainly involves starting
several Websocket and SSE endpoints. This process is enclosed in a Docker container and can be set up using the following command:
```
$ docker compose up -d
```

## Cross-compilation
The project is enabled to work with three different versions of Scala (i.e., 2.12.x, 2.13.x and 3.x) so it can be used across different
Scala environments. Therefore, all the commands will work by default with the 3.x version but it is possible to run the same command
for all the versions at the same time or just for one specific version. Below you can see how to do so with the test command.

Testing against all the cross-compiled versions:
```
$ sbt "+ test"
```

Testing against a specific version where `<version>` is one of the configured versions in the build.sbt file:
```
$ sbt "++<version> test"
```

### Dependencies
The following dependencies are used by this library:

| Dependency                                 | License                                 |
|--------------------------------------------|-----------------------------------------|
| info.picocli / picocli                     | Apache License 2.0                      |
| com.herminiogarcia / shexml                | MIT License                             |
| com.softwaremill.sttp.client4 / core       | Apache License 2.0                      |
| com.softwaremill.sttp.client4 / monix      | Apache License 2.0                      |
| io.monix / monix                           | Apache License 2.0                      |
| io.reactivex.rxjava3 / rxjava              | Apache License 2.0                      |
| info.picocli / picocli                     | Apache License 2.0                      |
| com.typesafe.scala-logging / scala-logging | Eclipse Public License v1.0 or LGPL-2.1 |
