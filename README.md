# ParCorr
This code evaluates the methods proposed in the paper ParCorr: Efficient Parallel Methods to Identify Similar Time Series Pairs across Sliding Windows.

Sliding windows are simulated on a static dataset by horizontally slicing the time series RDD.
Although not a streaming implementation, this does not compromise the evaluation of all the aspects of the method, namely:
  the incremental normalization / sketch computation, communication strategies, and functionality.
This setup allows to better evaluate the performance gains of the approach without depending on the specific characteristics or optimization of any dedicated streaming environment.

# Installation
Prerequisites:
  - Maven
  - Apache Spark (2.11)

Compilation:
  mvn package

# Usage
/tmp/spark/bin/spark-submit --class fr.inria.zenith.ParCorr ParCorr-1.0-SNAPSHOT-jar-with-dependencies.jar -help
/tmp/spark/bin/spark-submit --class fr.inria.zenith.ParCorr ParCorr-1.0-SNAPSHOT-jar-with-dependencies.jar -tsFilePath <path to input file>

