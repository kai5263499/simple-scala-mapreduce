# simple-scala-mapreduce

This is a simple Hadoop map/reduce job written in Scala. This job also makes use of Maven for dependency management and junit for unit testing the mapper and repper and the whole job.

## Building

To generate a Hadoop-runnable jar you can use the regular maven goals

    mvn package

## Testing

And to test, of course

    mvn test

I also added support for DistributedCache so that a list of regular expressions can be packaged up and distributed throughout the cluster and used in the mapper. Sure, this does violate some of the simplicity I promised but I needed it to solve a project I was working on.

To run this particular job on a cluster

    hadoop -jar target/simple-scala-mapreduce-1.0-jar-with-dependencies.jar src/test/resources/input/regexes src/test/resources/input/sample src/test/resources/ouput