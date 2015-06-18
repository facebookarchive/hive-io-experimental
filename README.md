- - -

**_This project is not actively maintained. Proceed at your own risk!_**

- - -  

# Overview #

[![Build Status](https://travis-ci.org/facebook/hive-io-experimental.png?branch=master)](https://travis-ci.org/facebook/hive-io-experimental)

A Hive Input/Output Library in Java.

The Hive project is built to be used through command line HQL.<br/>
As such it does not have a good interface to connect to it through code directly.<br/>
This project fixes that problem - it adds support for input/output with Hive in code directly.

Currently we have:<br/>
1. Simple APIs to read and write a table in a single process.<br/>
2. Hadoop compatible Input/OutputFormat that can interact with Hive tables.<br/>
3. __hivetail__ executable for dumping data from a Hive table to stdout.<br/>
4. Examples of HiveIO usage for building applications.<br/>

# Building #

This project uses [Maven](http://maven.apache.org/) for its build.<br/>
To build the code yourself clone this repository and run `mvn clean install`

# Using #
To use this library in your project with maven add this to you pom.xml:

    <dependency>
        <groupId>com.facebook.hiveio</groupId>
        <artifactId>hive-io-exp-core</artifactId>
        <version>0.10</version>
    </dependency>

You can also browse the
[released builds](http://goo.gl/6EdWN)
and download the jars directly.

# Reference #

The project's [maven site](http://facebook.github.io/hive-io-experimental/)
is generated with each release.<br/>
API information is available here: 
[JavaDocs](http://facebook.github.io/hive-io-experimental/apidocs/index.html).

# 1. Simple API #
If you just want to quickly read or write a Hive table from a Java process, these are for you.

The input API is simply:

    public class HiveInput {
      public static Iterable<HiveReadableRecord> readTable(HiveInputDescription inputDesc);
    }

And the output API is:

    public class HiveOutput {
      public static void writeTable(
            HiveOutputDescription outputDesc,
            Iterable<HiveWritableRecord> records)
    }

For example usages of these take a look at the cmdline apps and the tests in the code.

# 2. Hadoop Compatible API #

## Design ##
The input and output classes have a notion of profiles.
A profile is tagged by a name and describes the input or output you wish to connect to.
Profiles are serialized into the Hadoop Configuration so that they can be passed around easily.
MapReduce takes classes you configure it with and instantiates them using reflection on many hosts.

This sort of model makes it hard to configure the classes beyond just setting which to use.
The approach we have chosen is to have the user create input / output descriptions, and set them with a profile.
This writes it to the configuration which gets shipped to every worker by MapReduce.
See example code mentioned below for more information.

## Input ##
The Hive IO library conforms to Hadoop's
[InputFormat](http://hadoop.apache.org/docs/r0.23.6/api/org/apache/hadoop/mapreduce/InputFormat.html) API.
The class that implements this API is
[HiveApiInputFormat](hive-io-exp-core/src/main/java/com/facebook/hiveio/input/HiveApiInputFormat.java).
MapReduce creates the InputFormat using reflection.
It will call getSplits() to generate splits on the hive tables to read.
Each split is then sent to some worker, which then calls createRecordReader(split).
This creates a
[RecordReaderImpl](hive-io-exp-core/src/main/java/com/facebook/hiveio/input/RecordReaderImpl.java)
which is used to read each record.

## Output ##
The Hive IO library conforms to Hadoop's
[OutputFormat](http://hadoop.apache.org/docs/r0.23.6/api/org/apache/hadoop/mapreduce/OutputFormat.html) API.
The class that implements this API is
[HiveApiOutputFormat](hive-io-exp-core/src/main/java/com/facebook/hiveio/output/HiveApiOutputFormat.java).
<br/>
MapReduce creates the OutputFormat using reflection.<br/>
It will call checkOutputSpecs() to verify that the output can be written.<br/>
Then it will call createRecordWriter() on each map task host.
These record writers will get passed each record to write.<br/>
Finally it will call getOutputCommitter() and finalize the output.<br/>
Because of its use of reflection, to make use of this library you will need to extend HiveApiOutputFormat (and potentially HiveApiRecordWriter).

The best way to get started with output is to have a look at the example mapreduce
[code](hive-io-exp-mapreduce/src/main/java/com/facebook/hiveio/).
<br/>
This is a simple example of using the library that should be used as a starting point for projects. 

## Benchmarks ##
Using Hive IO we were able to read from Hive at **140 MB/s**.
For comparison, using the hive command line on the same partition reads at around 35 MB/s.
This benchmark is an evolving piece of work and we still have performance tweaks to make.
Code for the benchmark is located [here](hive-io-exp-cmdline/src/main/java/com/facebook/hiveio/benchmark/).

# 3. HiveTail #
This library comes with a hive reader, called hivetail, that can be used to stream through Hive data.<br/>
It reads from a Hive table / partition and dumps rows to stdout.<br/>
It is also a good starting point for implementing input with this library.<br/>
The code is located
[here](hive-io-exp-cmdline/src/main/java/com/facebook/hiveio/tailer/).<br/>
To use it first build the code or download the jar from
[Maven Central](http://goo.gl/ng9XA).
Then on your machine run `java -jar hive-io-exp-cmdline-0.10-jar-with-dependencies.jar help tail`.

# 4. Examples #

The [cmdline](hive-io-exp-cmdline/src/main/java/com/facebook/hiveio/)
module contains a suite of command line tools using HiveIO.<br/>
The jar-with-dependencies that it builds is completely standalone.<br/>
It includes hive jars and everything else it needs so that it can be run directly on the command line.<br/>
Moreover it does not use mapreduce, but rather executes completely in a single JVM.

This package contains:<br/>
1. A tool that writes to a predefined Hive table with multiple threads.<br/>
2. The tailer, see hivetail above.<br/>
3. Input benchmark, see above.<br/>

Additionally there is a [mapreduce](hive-io-exp-mapreduce/src/main/java/com/facebook/hiveio/mapreduce)
module that contains a suite of tools for MapReduce jobs with HiveIO.<br/>
Currently it contains an example MR job that writes to a Hive table.

## Real World Examples ##
[Giraph](http://giraph.apache.org/) uses Hive IO to read and write graphs from Hive.
It is another good example that can be used for starter code.<br/>
It is a more complicated use case than the examples mentioned above as it converts records to graph vertex and edge objects.<br/>
It has scaled to terabytes of data and is running in production at large companies (e.g. Facebook).<br/>
The code is
[here](http://goo.gl/4qsvX).
Specifically you should look at
[vertex-input](http://goo.gl/Aq8kV),
[edge-input](http://goo.gl/PGvrq),
and [output](http://goo.gl/PjI3b).
<br/>
Note especially how the vertex and edge inputs use different profiles to allow reading from multiple Hive tables.
