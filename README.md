# CASPITA: Mining Statistically Significant Paths in Time Series Data from an Unknown Network
## Authors: Andrea Tonon (andrea.tonon@dei.unipd.it) and Fabio Vandin (fabio.vandin@unipd.it)
This repository contains the implementation of the algorithm introduced in the IEEE ICDM 2021 paper *CASPITA: Mining Statistically Significant Paths in Time Series Data from an Unknown Network*, the code that has been used to test its performance, and an extended version of the [paper](CASPITA_Extended.pdf).

The code has been developed in Java and executed using version 1.8.0_201. We used the [fastutil library](http://fastutil.di.unimi.it) that provides type-specific maps, sets, lists and queues with a small memory footprint and fast access and insertion, for the data structures.

## Package Description
The package contains the following folders:
*	src/: contains the source code of CASPITA which considers the Binomial distribution to approximate the p-values
*	lib/: contains the jar of the libraries
*	reproducibility: contains the implementation of all the versions of CASPITA and the code and the explanations to reproduce all the results shown in the paper.

In addition, it contains:
*	an extended version of our [paper](CASPITA_Extended.pdf)
*	a [jar file](CASPITA.jar) to directly execute our algorithm CASPITA.

## Usage of CASPITA
We provide the source code and an executable jar file of the version of CASPITA which uses the Binomial distribution to approximate the p-values of the paths, which resulted as best version from our experimental evaluation shown in the paper.

### Compile
These are the instructions to compile the code (from the CASPITA folder):

```
javac -cp "./lib/*" src/Caspita_POG_Bin.java
```

### Dataset Format
The input file must contain each path in a different line and each vertex must be separated with a space from the following one.

E.g.:
```
1 2 3 4
2 3 5
1 3 4
```

### Execute from Compiled Code
These are the instructions to execute the compiled code (from the src folder). You can mine over or under represented paths, or both simultaneously. Let us note that the version of CASPITA which mines over and under represented paths simultaneously is provided to speed up the execution if one is interested in both types of paths but the FWER guarantees are valid individually for the over and under represented paths.

#### Mine Both Over and Under Represented Paths

It mines both over and under represented paths from the input file, and reports the over and under represented paths in the two output files. At standard output, it writes details of the datasets, of the generative null model, and of the executions. The parameters are the ones described in the paper. 

```
java -XmxRG -cp "../lib/*:." Caspita_POG_Bin inputFile P k h FWER BOTH outputFileOVER outputFileUNDER
```

-XmxRG allows to specify the maximum memory allocation pool for the Java Virtual Machine (JVM). R must be replaced with an integer that represents the maximum memory in GB (e.g., 50G).

E.g.: 

```
java -Xmx20G -cp "../lib/*:." Caspita_POG_Bin ../data/BIKE.txt 100 2 1 0.05 BOTH ../res/BIKE_OV_2_1.txt ../res/BIKE_UN_2_1.txt
```
#### Mine Over or Under Represented Paths

It mines over or under represented paths from the input file, and reports the over or under represented paths in the output file. At standard output, it writes details of the datasets, of the generative null model, and of the executions. The parameters are the ones described in the paper. 

```
java -XmxRG -cp "../lib/*:." Caspita_POG_Bin inputFile P k h FWER OVER/UNDER outputFile
```

-XmxRG allows to specify the maximum memory allocation pool for the Java Virtual Machine (JVM). R must be replaced with an integer that represents the maximum memory in GB (e.g., 50G).

E.g.: 

```
java -Xmx20G -cp "../lib/*:." Caspita_POG_Bin ../data/BIKE.txt 100 2 1 0.05 OVER ../res/BIKE_OV_2_1.txt
```

### Execute from Jar File
These are the instructions to execute the CASPITA from the JAR file (from the CASPITA folder). You can mine over or under represented paths, or both simultaneously. Let us note that the version of CASPITA which mines over and under represented paths simultaneously is provided to speed up the execution if one is interested in both types of paths but the FWER guarantees are valid individually for the over and under represented paths.

```
java -XmxRG -jar CASPITA.jar inputFile P k h FWER BOTH/OVER/UNDER outputFile1 [outputFile2]
```
The parameters are the same shown above for the execution from the compiled code.


##  Reproducibility
In the reproducibility folder, you can find the implementation and all the explanations to reproduce the results shown in the paper. 


## License
This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.
