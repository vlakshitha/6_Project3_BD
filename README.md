Distributed Image Processing with Kafka

A fault-tolerant distributed system that processes large images in parallel using Apache Kafka as the message broker. The system automatically splits images into tiles, distributes processing across worker nodes, and reconstructs the final result.

Overview

This project demonstrates a scalable microservices architecture where a master node coordinates image processing tasks across multiple worker nodes through Kafka topics. Users upload images via a web interface, and the system returns grayscale-processed results after parallel computation.

Key Components:
- Web Interface: Flask-based UI for image upload and result download
- Master Node: Orchestrates job splitting, task distribution, and result aggregation
- Kafka Broker: Manages message routing between master and workers
- Worker Nodes: Process image tiles independently and publish results
- Monitoring: Real-time worker health tracking via heartbeat messages

System Architecture

The pipeline operates across 4 nodes:

Node 1 (Master & Client) handles user interaction through a Flask web application. It receives uploaded images, splits them into 512×512 pixel tiles, and publishes tasks to Kafka's tasks topic. The master also subscribes to the results topic to collect processed tiles, reconstructs the complete image, and monitors worker availability through the heartbeats topic.

Node 2 (Kafka Broker) acts as the messaging backbone. It manages three topics: tasks for distributing work, results for collecting processed data, and heartbeats for worker monitoring. The broker uses 2 partitions for load balancing between workers and ensures message persistence for fault tolerance.

Node 3 & 4 (Workers) operate as independent processing units within the same Kafka consumer group. Each worker consumes tiles from assigned partitions, converts them to grayscale using PIL, and publishes results back to Kafka. Workers send periodic heartbeat messages to signal active status and enable fault detection.

Team Roles

Node 1 - Master & Client Interface - Lakshitha - CS665

Responsibilities:
- Design and implement Flask web interface for image upload/download
- Develop image splitting algorithm (512×512 tiling with metadata tracking)
- Create Kafka producer logic for task distribution
- Build result consumer to aggregate processed tiles
- Implement image reconstruction from received tiles
- Monitor worker heartbeats and display active worker status
- Handle job status tracking and progress reporting

Node 2 - Kafka Broker Infrastructure - Umar - CS662

Responsibilities:
- Install and configure Apache Kafka and Zookeeper
- Create and manage topics (tasks, results, heartbeats)
- Configure partitioning strategy for load balancing (2 partitions)
- Set retention policies for message persistence
- Ensure broker reliability and monitor Kafka health
- Troubleshoot consumer group coordination
- Manage topic deletion and configuration updates

Node 3 - Worker Node 1 - Tarun Basavaraju - CS644

Responsibilities:
- Implement Kafka consumer for tasks topic
- Develop image processing pipeline (grayscale conversion)
- Encode/decode image tiles using base64 for Kafka transmission
- Publish processed results to results topic
- Send periodic heartbeat messages to heartbeats topic
- Handle errors gracefully and log processing metrics
- Join consumer group for automatic load balancing

Node 4 - Worker Node 2 - Sourabh Hegde - CS929

Responsibilities:
- Implement Kafka consumer for tasks topic
- Develop image processing pipeline (grayscale conversion)
- Encode/decode image tiles using base64 for Kafka transmission
- Publish processed results to results topic
- Send periodic heartbeat messages to heartbeats topic
- Handle errors gracefully and log processing metrics
- Join consumer group for automatic load balancing

Technical Highlights

Scalability: Workers can be added horizontally without code changes. Kafka automatically rebalances partitions.

Fault Tolerance: Message persistence ensures no data loss. If a worker crashes, its tasks are reassigned to active workers.

Parallel Processing: Consumer groups enable concurrent tile processing, reducing total job time proportional to worker count.

Asynchronous Architecture: Kafka decouples producers and consumers, allowing independent scaling and deployment.

Real-time Monitoring: Heartbeat mechanism provides visibility into worker health and system capacity.

Technology Stack

- Apache Kafka 3.9 - Distributed message broker
- Python 3.8+ - Application language
- Flask - Web framework
- Pillow (PIL) - Image processing
- kafka-python - Kafka client library

Course: Big Data - Project 3  
Institution: PES University  
Year: 2025

This project fulfills distributed systems requirements: multi-node architecture, asynchronous messaging, parallel processing, fault tolerance, and web-based monitoring.
