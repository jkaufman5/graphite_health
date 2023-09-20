#!/bin/bash

IMAGE="patient_analysis"

# Build Docker image
docker build -t $IMAGE .

# Run Docker container
docker run -v "$(pwd)":/app $IMAGE
