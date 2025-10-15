#!/bin/bash

set -e  # exit on error

# Parse input arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --user) USERNAME="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ -z $USERNAME ]; then 
    echo "You need to pass --user to the script."
    exit 1
fi

REGISTRY=ic-registry.epfl.ch
IMG_NAME=mlo/$USERNAME/llm4medicalguideline
VERSION=1

docker build . -t $IMG_NAME:$VERSION
docker tag $IMG_NAME:$VERSION $REGISTRY/$IMG_NAME:$VERSION
docker tag $IMG_NAME:$VERSION $REGISTRY/$IMG_NAME:latest
docker push $REGISTRY/$IMG_NAME:$VERSION
docker push $REGISTRY/$IMG_NAME:latest
docker rmi $REGISTRY/$IMG_NAME:$VERSION
docker rmi $REGISTRY/$IMG_NAME:latest