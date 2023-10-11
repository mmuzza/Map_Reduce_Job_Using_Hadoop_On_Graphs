Description: Map Reducer For Net Game Sim 
Name: Muhammad Muzzammil

Instructions:

Begin by setting up your development environment, which includes installing IntelliJ, JDK, Scala runtime, the IntelliJ Scala plugin, and the Simple Build Toolkit (SBT). Ensure that you have also configured Scala monitoring tools for proper functionality.
Once the environment is set up, launch IntelliJ and open my project. Initiate the project build process. Please be patient as this step may take some time, as it involves downloading and incorporating the library dependencies listed in the build.sbt file into the project's classpath.
If you prefer an alternative approach, you can run the project via the command line. Open a terminal and navigate to the project directory for this purpose.
The package includes one main class, one map reducer class for nodes, one map reducer class for edges, and one sharding class. Everything is ran through the “sbt clean compile assembly” command in the terminal.
Before running the command, you must go to “application.conf” which can be found under source>main>application.conf and change the following directories to your computer directory:
originalPerturbedNodes: (This is the output directory->cartesian product of OxP Nodes)
InputToShardNodes: (This can be originalPerturbedNodes)
outputForShardNodes: (This is the output directory for sharded nodes)
originalPerturbedEdges:  (This is the output directory->cartesian product of OxP Edges)
inputToShardEdges: (This can be originalPerturbedEdges)
outputForShardEdges: (This is the output directory for sharded nodes)
originalNgs: (This is the Ngs file for original graph)
originalNgsDirectory: (Directory to where to find the Original ngs file)
PerturbedNgs: (This is the Ngs file for perturbed graph)
perturbedNgsDirectory: (Directory to where to find the Perturbed ngs file)
