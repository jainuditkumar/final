#!/bin/bash

# Exit on error
set -e

# Load environment variables
if [ -f ../.env ]; then
    source ../.env
fi

echo "Setting up RabbitMQ cluster..."

# Start the docker-compose services
echo "Starting docker containers..."
docker-compose -f ../docker/docker-compose.yml up -d

# Wait for RabbitMQ nodes to be ready
echo "Waiting for RabbitMQ nodes to initialize (30s)..."
sleep 30

# Check if nodes are running
echo "Checking if RabbitMQ nodes are running..."
docker exec rabbitmq-node-1 rabbitmqctl cluster_status

# Join nodes to form a cluster
echo "Creating RabbitMQ cluster..."

# Stop the app on node2 and node3
echo "Stopping rabbit application on node2 and node3..."
docker exec rabbitmq-node-2 rabbitmqctl stop_app
docker exec rabbitmq-node-3 rabbitmqctl stop_app

# Join node2 to node1
echo "Joining node2 to the cluster..."
docker exec rabbitmq-node-2 rabbitmqctl reset
docker exec rabbitmq-node-2 rabbitmqctl join_cluster rabbit@rabbitmq-node-1
docker exec rabbitmq-node-2 rabbitmqctl start_app

# Join node3 to node1
echo "Joining node3 to the cluster..."
docker exec rabbitmq-node-3 rabbitmqctl reset
docker exec rabbitmq-node-3 rabbitmqctl join_cluster rabbit@rabbitmq-node-1
docker exec rabbitmq-node-3 rabbitmqctl start_app

# Set cluster policy for high availability
echo "Setting HA policy for queues..."
docker exec rabbitmq-node-1 rabbitmqctl set_policy ha-all ".*" '{"ha-mode":"all", "ha-sync-mode":"automatic"}' --apply-to queues

# Check final cluster status
echo "Checking final cluster status..."
docker exec rabbitmq-node-1 rabbitmqctl cluster_status

echo "RabbitMQ cluster setup complete!"
echo "Management UI is available at http://localhost:15672"
echo "Username: guest (or as configured in .env)"
echo "Password: guest (or as configured in .env)"
