#!/bin/bash

# display only the name of the current branch you're on
echo "Checking the current branch..."
git rev-parse --abbrev-ref HEAD

echo "Fetching the latest changes..."
git fetch

echo "Checking out the 'main' branch..."
git checkout main

echo "Pulling the latest changes..."
git pull

# Read the .env file into stdout with "cat .env", filter out lines that start with "#",
# and pipe it to xargs, which builds an argument list out the standard input.
# cat .env | grep -v "^#"
export $(cat .env | grep -v "^#" | xargs)

# Build and tag image locally in one step.
# No need for docker tag <image> mccarthysean/ijack:<tag>

echo ""
echo "Pulling the base 'builder' image from Docker Hub..."
echo "docker pull mccarthysean/ijack:postgresql_scheduler_base || true"
docker pull mccarthysean/ijack:postgresql_scheduler_base || true

echo ""
echo "Pulling the final 'production' image from Docker Hub..."
echo "docker pull mccarthysean/ijack:postgresql_scheduler_final || true"
docker pull mccarthysean/ijack:postgresql_scheduler_final || true

# Enable exit on non 0 AFTER the Docker pull commands
set -e

echo ""
echo "Building the base 'builder' image locally..."
echo "docker compose -f docker-compose.build.prod.base.yml build"
docker compose -f docker-compose.build.prod.base.yml build

echo ""
echo "Pushing the base 'builder' image to Docker Hub..."
echo "docker push mccarthysean/ijack:postgresql_scheduler_base"
docker push mccarthysean/ijack:postgresql_scheduler_base

echo ""
echo "Building the final 'production' image locally..."
echo "docker compose -f docker-compose.build.prod.final.yml build"
docker compose -f docker-compose.build.prod.final.yml build

echo ""
echo "Pushing the final 'production' image to Docker Hub..."
echo "docker push mccarthysean/ijack:postgresql_scheduler_final"
docker push mccarthysean/ijack:postgresql_scheduler_final

# Deploy to the Docker swarm and send login credentials
# to other nodes in the swarm with "--with-registry-auth"
echo ""
echo "Deploying to the Docker swarm..."
echo "docker stack deploy --with-registry-auth -c docker-compose.prod.yml postgresql_scheduler"
docker stack deploy --with-registry-auth -c docker-compose.prod.yml postgresql_scheduler

echo ""
echo "Deployment complete!"

exit 0
