#!/bin/bash

# Build and tag image locally in one step. 
# No need for docker tag <image> mccarthysean/ijack:<tag>
echo ""
echo "Building the image locally..."
echo "docker compose -f docker compose.build.yml build"
docker compose -f docker compose.build.yml build

# Push to Docker Hub
# docker login --username=mccarthysean
echo ""
echo "Pushing the image to Docker Hub..."
echo "docker push mccarthysean/ijack:postgresql_scheduler"
docker push mccarthysean/ijack:postgresql_scheduler

# Deploy to the Docker swarm and send login credentials 
# to other nodes in the swarm with "--with-registry-auth"
echo ""
echo "Deploying to the Docker swarm..."
echo "docker stack deploy --with-registry-auth -c docker compose-prod.yml postgresql_scheduler"
docker stack deploy --with-registry-auth -c docker compose-prod.yml postgresql_scheduler
