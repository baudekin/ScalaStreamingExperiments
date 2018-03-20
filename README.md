# ScalaStreamingExperiments

To build and run the scala spark examples:
#Run sbt clean package from the project directory
#To run the DStream example modify the following command for your machine: ~/servers/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class "com.github.baudekin.DStreamQueueExperiment" --master local[2] <path to repository>/ScalaStreamingExperiments/target/scala-2.11/scalastreamingexperiments_2.11-0.1.jar. Note must send kill signal to stop this application.
#To run StructuredStreamingStepExample modify the following command for your machine:~/servers/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class "com.github.baudekin.StructuredStreamingStepExample" --master local[2] /<path to repository>/ScalaStreamingExperiments/target/scala-2.11/scalastreamingexperiments_2.11-0.1.jar > a.out
