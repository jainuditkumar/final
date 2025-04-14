#!/bin/bash

# Exit on error
set -e

echo "Tearing down RabbitMQ cluster..."

# Check if docker-compose is running
if [ -z "$(sudo docker-compose -f ../docker/docker-compose.yml ps -q)" ]; then
    echo "No running containers found. Nothing to tear down."
    exit 0
fi

# Stop and remove containers
echo "Stopping and removing containers..."
sudo docker-compose -f ../docker/docker-compose.yml down

# Remove volumes (optional, uncomment to remove persistent data)
# echo "Removing volumes..."
# sudo docker-compose -f ../docker/docker-compose.yml down -v

# Check if any containers are still running
if [ -n "$(sudo docker ps -q --filter name=rabbitmq-node)" ]; then
    echo "Stopping any remaining RabbitMQ containers..."
    sudo docker stop $(sudo docker ps -q --filter name=rabbitmq-node)
fi

echo "RabbitMQ cluster tear down complete!"
