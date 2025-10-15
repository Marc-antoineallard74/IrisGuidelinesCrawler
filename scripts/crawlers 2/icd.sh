#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    source .env
else
    echo ".env file not found!"
    exit 1
fi

# Default values for the arguments
RELEASE=""
LANG=""
LINEARIZATION=""
ICD_VERSION=""
API_VERSION=""
OUTPUT_DIR=""
HF_REPO=""

# Parse command line arguments using getopts
while getopts "r:l:i:v:a:o:h:" opt; do
    case $opt in
        r) RELEASE="--release $OPTARG" ;;
        l) LANG="--lang $OPTARG" ;;
        i) LINEARIZATION="--linearization $OPTARG" ;;
        v) ICD_VERSION="--icd-version $OPTARG" ;;
        a) API_VERSION="--api-version $OPTARG" ;;
        o) OUTPUT_DIR="--output-dir $OPTARG" ;;
        h) HF_REPO="--hf-repo $OPTARG" ;;
        *) echo "Invalid option"; exit 1 ;;
    esac
done

COMMAND="python src/data/crawler/icd_crawler.py \
            $RELEASE \
            $LANG \
            $LINEARIZATION \
            $ICD_VERSION \
            $API_VERSION \
            $OUTPUT_DIR \
            $HF_REPO"

echo "Python command:"
echo $COMMAND

$COMMAND