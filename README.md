Description: Map Reducer For Net Game Sim

Name: Muhammad Muzzammil

Instructions:

Begin by setting up your development environment, which includes installing IntelliJ, JDK, Scala runtime, the IntelliJ Scala plugin, and the Simple Build Toolkit (SBT). Ensure that you have also configured Scala monitoring tools for proper functionality.

Once the environment is set up, launch IntelliJ and open my project. Initiate the project build process. Please be patient as this step may take some time, as it involves downloading and incorporating the library dependencies listed in the build.sbt file into the project's classpath.

If you prefer an alternative approach, you can run the project via the command line. Open a terminal and navigate to the project directory for this purpose.
The package includes one main class, one map reducer class for nodes, one map reducer class for edges, and one sharding class. Everything is ran through the “sbt clean compile assembly” command in the terminal.

You should use the original ReadMe file to generate the NGS file for both Original Graph and Perturbed Graph, 

Before running the command, you must go to “application.conf” which can be found under source>main>application.conf and change the following directories to your computer directory to ensure running the program smoothly:

1. originalPerturbedNodes: (This is the output directory->cartesian product of OxP Nodes)
2. InputToShardNodes: (This will be originalPerturbedNodes)
3. outputForShardNodes: (This is the output directory for sharded nodes)
4. originalPerturbedEdges:  (This is the output directory->cartesian product of OxP Edges)
5. inputToShardEdges: (This can be originalPerturbedEdges)
6. outputForShardEdges: (This is the output directory for sharded nodes)
7. originalNgs: (This is the Ngs file for original graph)
8. originalNgsDirectory: (Directory to where to find the Original ngs file)
9. PerturbedNgs: (This is the Ngs file for perturbed graph)
10. perturbedNgsDirectory: (Directory to where to find the Perturbed ngs file)
11. nodesMapReduceInputPath: (This is the directory for the map reducer for nodes --> the input file folder or file)
12. nodesMapReduceOutputPath: (This is the directory output folder for the map reducer ran on the files for sharded nodes)
13. edgesMapReduceInputPath: (This is the directory for the map reducer for edges --> the input file folder or file)
14. edgesMapReduceOutputPath: (This is the directory output folder for the map reducer ran on the files for sharded edges)

After these has been set, the program is ready to run


You can run the Map Reducer Job in two ways:

1. First being locally
   - You will have to configure the files in the NodesMapReduce and EdgesMapReduce
   - Make sure you have hadoop installed on your computer
  
2. Second being on AWS
   - You will have to create the jar file for the main class you want to run
   - For that please set one of the map reduce classes as main in the sbt file
   - Then press the command sbt clean compile assembly
   - That will generate the jar file NetGameSim.jar
   - Use that file to upload to AWS and the sharded files as the input which you produced running the main.scala to the folder you chose in the directory of applications.conf

  
This project has a total of 5 tests in Scala. In order to run them using the terminal, cd into the project directory and run the command sbt clean compile test

