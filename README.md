# 🎮 NetGameSim MapReduce

A distributed graph processing framework using Apache Hadoop MapReduce to analyze and compare original and perturbed network game simulation graphs at scale.

[![Scala](https://img.shields.io/badge/Scala-2.13-red.svg)](https://www.scala-lang.org/)
[![Hadoop](https://img.shields.io/badge/Hadoop-3.x-yellow.svg)](https://hadoop.apache.org/)
[![SBT](https://img.shields.io/badge/SBT-1.x-blue.svg)](https://www.scala-sbt.org/)

**Author:** Muhammad Muzzammil

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Local Execution](#local-execution)
  - [AWS EMR Deployment](#aws-emr-deployment)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [How It Works](#how-it-works)
- [Output](#output)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

NetGameSim MapReduce is a scalable graph analysis system that processes large-scale network game simulation graphs using distributed computing. The application performs comparative analysis between original and perturbed graphs by:

1. Computing Cartesian products of original and perturbed nodes/edges
2. Sharding large datasets into manageable chunks for distributed processing
3. Running parallel MapReduce jobs to identify similarities and differences
4. Generating comprehensive analysis reports

This project demonstrates practical implementation of big data processing techniques for graph analytics, making it ideal for analyzing network structures, detecting perturbations, and understanding graph evolution patterns.

## ✨ Features

- **Distributed Graph Processing**: Leverages Hadoop MapReduce for parallel computation
- **Intelligent Sharding**: Automatically partitions large graphs into optimal shard sizes
- **Dual Graph Analysis**: Compares original and perturbed graph structures
- **Flexible Deployment**: Runs on local Hadoop clusters or AWS EMR
- **Cartesian Product Computation**: Efficiently computes node and edge combinations
- **Scalable Architecture**: Handles graphs with millions of nodes and edges
- **Comprehensive Testing**: Includes 5 test suites for reliability

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    NetGameSim MapReduce Pipeline                 │
└─────────────────────────────────────────────────────────────────┘

1. Input Phase
   ┌──────────────┐         ┌──────────────┐
   │  Original    │         │  Perturbed   │
   │  NGS Graph   │         │  NGS Graph   │
   └──────┬───────┘         └──────┬───────┘
          │                        │
          └────────────┬───────────┘
                       ▼
2. Cartesian Product Generation
   ┌────────────────────────────────┐
   │   Original × Perturbed         │
   │   - Nodes Cartesian Product    │
   │   - Edges Cartesian Product    │
   └────────────┬───────────────────┘
                ▼
3. Sharding Phase
   ┌────────────────────────────────┐
   │   Data Sharding                │
   │   - Node Shards                │
   │   - Edge Shards                │
   └────────────┬───────────────────┘
                ▼
4. MapReduce Execution
   ┌────────────────────────────────┐
   │   Parallel Processing          │
   │   - NodesMapReduce Job         │
   │   - EdgesMapReduce Job         │
   └────────────┬───────────────────┘
                ▼
5. Output Phase
   ┌────────────────────────────────┐
   │   Analysis Results             │
   │   - Node Comparisons           │
   │   - Edge Comparisons           │
   │   - Statistics & Metrics       │
   └────────────────────────────────┘
```

## 📦 Prerequisites

**Required Software:**
- **JDK**: Version 8 or higher
- **Scala**: Version 2.13.x
- **SBT**: Simple Build Toolkit 1.x
- **IntelliJ IDEA**: With Scala plugin (recommended)
- **Apache Hadoop**: 3.x (for local execution)
- **AWS CLI**: Configured with credentials (for AWS deployment)

**System Requirements:**
- Minimum 8GB RAM (16GB recommended for large graphs)
- 50GB available disk space
- Multi-core processor recommended

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd NetGameSimMapReduce
```

### 2. Set Up Development Environment

**Install IntelliJ IDEA and Scala Plugin:**
```bash
# Install IntelliJ IDEA Community Edition
# Add Scala plugin through: File → Settings → Plugins → Search "Scala"
```

**Install SBT:**
```bash
# macOS
brew install sbt

# Linux
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
sudo apt-get update
sudo apt-get install sbt

# Windows
# Download from https://www.scala-sbt.org/download.html
```

**Install Hadoop (for local execution):**
```bash
# Download and extract Hadoop 3.x
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
export HADOOP_HOME=/path/to/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin
```

### 3. Build the Project

```bash
cd NetGameSimMapReduce
sbt clean compile assembly
```

**Note:** Initial build may take several minutes as SBT downloads dependencies.

## ⚙️ Configuration

All configuration parameters are located in `src/main/resources/application.conf`. Update the following paths to match your system:

### Required Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `originalPerturbedNodes` | Output directory for O×P nodes Cartesian product | `/output/cartesian/nodes` |
| `InputToShardNodes` | Input directory for node sharding (use `originalPerturbedNodes`) | `/output/cartesian/nodes` |
| `outputForShardNodes` | Output directory for sharded nodes | `/output/shards/nodes` |
| `originalPerturbedEdges` | Output directory for O×P edges Cartesian product | `/output/cartesian/edges` |
| `inputToShardEdges` | Input directory for edge sharding (use `originalPerturbedEdges`) | `/output/cartesian/edges` |
| `outputForShardEdges` | Output directory for sharded edges | `/output/shards/edges` |
| `originalNgs` | Filename of original NGS graph | `original.ngs` |
| `originalNgsDirectory` | Directory path to original NGS file | `/data/graphs/` |
| `PerturbedNgs` | Filename of perturbed NGS graph | `perturbed.ngs` |
| `perturbedNgsDirectory` | Directory path to perturbed NGS file | `/data/graphs/` |
| `nodesMapReduceInputPath` | Input path for nodes MapReduce job | `/output/shards/nodes` |
| `nodesMapReduceOutputPath` | Output path for nodes MapReduce results | `/output/results/nodes` |
| `edgesMapReduceInputPath` | Input path for edges MapReduce job | `/output/shards/edges` |
| `edgesMapReduceOutputPath` | Output path for edges MapReduce results | `/output/results/edges` |

### Sample Configuration

```hocon
application {
  # Cartesian Product Outputs
  originalPerturbedNodes = "/home/user/netgamesim/output/cartesian/nodes"
  originalPerturbedEdges = "/home/user/netgamesim/output/cartesian/edges"
  
  # Sharding Configuration
  InputToShardNodes = "/home/user/netgamesim/output/cartesian/nodes"
  outputForShardNodes = "/home/user/netgamesim/output/shards/nodes"
  inputToShardEdges = "/home/user/netgamesim/output/cartesian/edges"
  outputForShardEdges = "/home/user/netgamesim/output/shards/edges"
  
  # Input NGS Files
  originalNgs = "original_graph.ngs"
  originalNgsDirectory = "/home/user/netgamesim/data/"
  PerturbedNgs = "perturbed_graph.ngs"
  perturbedNgsDirectory = "/home/user/netgamesim/data/"
  
  # MapReduce Paths
  nodesMapReduceInputPath = "/home/user/netgamesim/output/shards/nodes"
  nodesMapReduceOutputPath = "/home/user/netgamesim/results/nodes"
  edgesMapReduceInputPath = "/home/user/netgamesim/output/shards/edges"
  edgesMapReduceOutputPath = "/home/user/netgamesim/results/edges"
}
```

## 💻 Usage

### Generate NGS Graph Files

Before running the MapReduce pipeline, generate NGS graph files for both original and perturbed graphs using the NetGameSim tool (refer to the original NetGameSim documentation).

### Local Execution

**Step 1: Configure for Local Execution**

Ensure Hadoop is properly configured on your local machine and update paths in `application.conf` to use local file system paths.

**Step 2: Run the Complete Pipeline**

```bash
# Build the project
sbt clean compile assembly

# Run the main pipeline
sbt run

# Or run specific MapReduce jobs
# For nodes analysis
sbt "runMain NodesMapReduce"

# For edges analysis
sbt "runMain EdgesMapReduce"
```

**Step 3: Monitor Execution**

Check Hadoop logs and web UI (typically at `http://localhost:8088`) to monitor job progress.

### AWS EMR Deployment

**Step 1: Build JAR File**

```bash
# Update build.sbt to set the desired main class
# For nodes processing:
mainClass in assembly := Some("NodesMapReduce")

# Build the assembly JAR
sbt clean compile assembly

# JAR will be created at: target/scala-2.13/NetGameSim.jar
```

**Step 2: Upload Data to S3**

```bash
# Upload sharded input files
aws s3 cp output/shards/nodes/ s3://your-bucket/input/nodes/ --recursive
aws s3 cp output/shards/edges/ s3://your-bucket/input/edges/ --recursive
```

**Step 3: Create EMR Cluster**

```bash
aws emr create-cluster \
  --name "NetGameSim MapReduce Cluster" \
  --release-label emr-6.10.0 \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes KeyName=your-key-pair \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles
```

**Step 4: Submit MapReduce Job**

```bash
# Upload JAR to S3
aws s3 cp target/scala-2.13/NetGameSim.jar s3://your-bucket/jars/

# Add step to EMR cluster
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=CUSTOM_JAR,Name="Nodes MapReduce",\
ActionOnFailure=CONTINUE,\
Jar=s3://your-bucket/jars/NetGameSim.jar,\
Args=[s3://your-bucket/input/nodes,s3://your-bucket/output/nodes]
```

**Step 5: Monitor and Retrieve Results**

```bash
# Check step status
aws emr describe-step --cluster-id j-XXXXXXXXXXXXX --step-id s-XXXXXXXXXXXXX

# Download results
aws s3 cp s3://your-bucket/output/nodes/ ./results/nodes/ --recursive
```

## 📁 Project Structure

```
NetGameSimMapReduce/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── Main.scala              # Main orchestration class
│   │   │   ├── NodesMapReduce.scala    # MapReduce job for nodes
│   │   │   ├── EdgesMapReduce.scala    # MapReduce job for edges
│   │   │   └── ShardingUtils.scala     # Data sharding utilities
│   │   └── resources/
│   │       └── application.conf        # Configuration file
│   └── test/
│       └── scala/
│           └── TestSuite.scala         # Test cases
├── build.sbt                           # SBT build configuration
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── target/
│   └── scala-2.13/
│       └── NetGameSim.jar             # Assembled JAR file
└── README.md                           # This file
```

## 🧪 Testing

The project includes 5 comprehensive test suites covering:
- Cartesian product generation
- Sharding logic
- MapReduce functionality
- Configuration validation
- Data integrity checks

**Run all tests:**
```bash
cd NetGameSimMapReduce
sbt clean compile test
```

**Run specific test:**
```bash
sbt "testOnly *TestClassName"
```

**Generate test coverage report:**
```bash
sbt clean coverage test coverageReport
```

## 🔍 How It Works

### 1. Cartesian Product Generation

The system creates all possible combinations of nodes and edges between the original and perturbed graphs:
- **Nodes**: Original_Node × Perturbed_Node
- **Edges**: Original_Edge × Perturbed_Edge

This allows for comprehensive comparison of graph structures.

### 2. Data Sharding

Large Cartesian products are split into manageable shards to enable parallel processing:
- Configurable shard sizes based on available cluster resources
- Even distribution across shards for load balancing
- Maintains data locality for optimization

### 3. MapReduce Processing

**Mapper Phase:**
- Reads sharded node/edge pairs
- Extracts relevant features and attributes
- Emits key-value pairs for comparison

**Reducer Phase:**
- Aggregates mapped data by key
- Performs similarity computations
- Generates comparison statistics

### 4. Result Aggregation

Final results include:
- Matched nodes/edges between graphs
- Similarity scores
- Statistical summaries
- Perturbation detection metrics

## 📊 Output

The MapReduce jobs produce the following outputs:

**Nodes Analysis Results:**
```
node_id_original,node_id_perturbed,similarity_score,attributes_matched
1,1,0.95,children:3,props:8
2,2,0.87,children:2,props:7
...
```

**Edges Analysis Results:**
```
edge_id_original,edge_id_perturbed,similarity_score,weight_diff
e1,e1,1.0,0.0
e2,e3,0.78,0.05
...
```

**Summary Statistics:**
- Total nodes/edges processed
- Match rate percentages
- Average similarity scores
- Processing time metrics

## 🔧 Troubleshooting

### Common Issues

**Issue:** `OutOfMemoryError` during Cartesian product generation
```bash
# Solution: Increase JVM heap size
export JAVA_OPTS="-Xmx8g -Xms4g"
sbt -mem 8192 run
```

**Issue:** Hadoop job fails with permission errors
```bash
# Solution: Check HDFS permissions
hadoop fs -chmod -R 755 /output/
```

**Issue:** AWS EMR step fails
```bash
# Solution: Check CloudWatch logs
aws logs tail /aws/emr/j-XXXXXXXXXXXXX/steps/s-XXXXXXXXXXXXX --follow
```

**Issue:** Configuration file not found
```bash
# Solution: Ensure application.conf is in resources directory
src/main/resources/application.conf
```

### Performance Optimization Tips

1. **Adjust Shard Size**: Optimize based on cluster size and data volume
2. **Configure Memory**: Allocate sufficient heap space for JVM
3. **Use Compression**: Enable intermediate data compression
4. **Partition Strategy**: Use appropriate partitioning for data distribution

## 📝 License

This project is available for academic and educational purposes.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## 📧 Contact

**Muhammad Muzzammil**

For questions or support, please open an issue on the repository.

---

**Built with ❤️ using Scala, Hadoop, and distributed computing principles**
