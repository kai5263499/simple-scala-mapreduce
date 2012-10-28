# simple-scala-mapreduce

This is a simple Hadoop map/reduce job written in Scala. This job also makes use of Maven for dependency management and junit for unit testing the mapper and repper and the whole job.

To generate a Hadoop-runnable jar

    mvn package

I also added support for DistributedCache so that a list of regular expressions can be packaged up and distributed throughout the cluster and used in the mapper. Sure, this does violate some of the simplicity I promised but I needed it to solve a project I was working on.