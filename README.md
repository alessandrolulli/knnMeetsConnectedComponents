# knnMeetsConnectedComponents

### Publications ###

Alessandro Lulli, Thibault Debatty, Laura Ricci, Matteo Dell’Amico, and Pietro Michiardi,
**Scalable k-NN based text clustering**,
Accepted ad IEEE BigData 2015

### How to build ###

The project can be built using Maven.
From the main dir:
mvn package

### How to run ###

The main class is: util.KnnMeetsConnectedComponents

It is possible to execute the job in two ways:
1) Submit a job to your Spark environment
2) use the script in run/runKnnMeetsConnectedComponents.sh

It is required also to provide under the lib folder the Spark lib.
A pre-built Spark lib can be downloaded from the following URL:
https://www.dropbox.com/s/xnfqs0ht4nqv5lc/spark-assembly-1.2.0-hadoop2.2.0.jar?dl=0

### Configuration ###

The application requires a configuration file.
An example of configuration file is: run/config_knnMeetsCC

### Dataset format ###

The application requires the following format:

vertexIdentifier*separator*stringValue

Where *separator* can be configured using the edgelistSeparator configuration variable
An example is: run/subjectSmall

### Contact ###

In case of any issues / suggestions or to have further details please contact: lulli@di.unipi.it
http://www.di.unipi.it/~lulli
