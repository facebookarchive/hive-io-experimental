# Overview #

[![Build Status](https://travis-ci.org/facebook/hive-io-experimental.png?branch=master)](https://travis-ci.org/facebook/hive-io-experimental)

A Hive Input/Output Library in Java.

The Hive project is built to be used through command line HQL and as such does
not have a good interface to connect to it through code directly. This project
adds support for input/output with Hive in code directly.

For starters this means a Java Hadoop compatible Input/OutputFormat that can
interact with Hive tables. In the future we will add other hooks for dealing
with Hive as demand grows.

# Building #

This project uses [Maven](http://maven.apache.org/) for its build.<br/>
To build the code yourself clone this repository and run `mvn clean install`

# Using #
To use this library in your project with maven add this to you pom.xml:

    <dependency>
      <groupId>com.facebook.giraph.hive</groupId>
      <artifactId>hive-io-exp-core</artifactId>
      <version>0.6</version>
    </dependency>

You can also browse the
[released builds](http://search.maven.org/#search%7Cga%7C1%7Ccom.facebook.giraph.hive)
and download the jars directly.

# Reference #

The project's [maven site](http://facebook.github.io/hive-io-experimental/) is generated with each release.
<br/>
API information is available here: 
[JavaDocs](http://facebook.github.io/hive-io-experimental/apidocs/index.html).

# Design #
The input and output classes have a notion of profiles.
A profile is tagged by a name and describes the input or output you wish to connect to.
Profiles are serialized into the Hadoop Configuration so that they can be passed around easily.
MapReduce takes classes you configure it with and instantiates them using reflection on many hosts.

This sort of model makes it hard to configure the classes beyond just setting which to use.
The approach we have chosen is to have the user create input / output descriptions, and set them with a profile.
This writes it to the configuration which gets shipped to every worker by MapReduce.
See example code mentioned below for more information.

# Input #
The Hive IO library conforms to Hadoop's
[OutputFormat](http://hadoop.apache.org/docs/r0.23.6/api/org/apache/hadoop/mapreduce/InputFormat.html) API.
The class that implements this API is
[HiveApiInputFormat](hive-io-exp-core/src/main/java/com/facebook/hiveio/input/HiveApiInputFormat.java).
MapReduce creates the InputFormat using reflection.
It will call getSplits() to generate splits on the hive tables to read.
Each split is then sent to some worker, which then calls createRecordReader(split).
This creates a
[RecordReaderImpl](hive-io-exp-core/src/main/java/com/facebook/hiveio/input/RecordReaderImpl.java)
which is used to read each record.

### HiveTail ###
This library comes with a hive reader, called hivetail, that can be used to stream through Hive data.
<br/>
It reads from a Hive table / partition and dumps rows to stdout.
<br/>
It is also a good starting point for implementing input with this library.
<br/>
The code is located
[here](hive-io-exp-cmdline/src/main/java/com/facebook/hiveio/tailer/).
<br/>
To use it first build the code or download the jar from
[Maven Central](http://search.maven.org/#artifactdetails%7Ccom.facebook.giraph.hive%7Chive-io-exp-tailer%7C0.6%7Cjar).
Then on your machine run `java -jar hive-io-exp-tailer-0.6-SNAPSHOT-jar-with-dependencies.jar -help`.

### Benchmark ###
Using Hive IO we were able to read from Hive at **140 MB/s**.
For comparison, using the hive command line on the same partition reads at around 35 MB/s.
This benchmark is an evolving piece of work and we still have performance tweaks to make.
Code for the benchmark is located [here](hive-io-exp-cmdline/src/main/java/com/facebook/hiveio/benchmark/).

# Output #
The Hive IO library conforms to Hadoop's
[OutputFormat](http://hadoop.apache.org/docs/r0.23.6/api/org/apache/hadoop/mapreduce/OutputFormat.html) API.
The class that implements this API is
[HiveApiOutputFormat](hive-io-exp-core/src/main/java/com/facebook/hiveio/output/HiveApiOutputFormat.java).
<br/>
MapReduce creates the OutputFormat using reflection.
<br/>
It will call checkOutputSpecs() to verify that the output can be written.
<br/>
Then it will call createRecordWriter() on each map task host.
These record writers will get passed each record to write.
<br/>
Finally it will call getOutputCommitter() and finalize the output.
<br/>
Because of its use of reflection, to make use of this library you will need to extend HiveApiOutputFormat (and potentially HiveApiRecordWriter).

The best way to get started with output is to have a look at the example mapreduce
[code](hive-io-exp-mapreduce/src/main/java/com/facebook/hiveio/).
<br/>
This is a simple example of using the library that should be used as a starting point for projects. 

# Real World Examples #
[Giraph](http://giraph.apache.org/) uses Hive IO to read and write graphs from Hive.
It is another good example that can be used for starter code.
<br/>
It is a more complicated use case than the examples mentioned above as it converts records to graph vertex and edge objects.
<br/>
It has scaled to terabytes of data and is running in production at large companies (e.g. Facebook).
<br/>
The code is
[here](https://github.com/apache/giraph/tree/trunk/giraph-hive/src/main/java/org/apache/giraph/hive).
Specifically you should look at
[vertex-input](https://github.com/apache/giraph/blob/trunk/giraph-hive/src/main/java/org/apache/giraph/hive/input/vertex/HiveVertexInputFormat.java),
[edge-input](https://github.com/apache/giraph/blob/trunk/giraph-hive/src/main/java/org/apache/giraph/hive/input/edge/HiveEdgeInputFormat.java),
and [output](https://github.com/apache/giraph/blob/trunk/giraph-hive/src/main/java/org/apache/giraph/hive/output/HiveVertexOutputFormat.java).
<br/>
Note especially how the vertex and edge inputs use different profiles to allow reading from multiple Hive tables.
