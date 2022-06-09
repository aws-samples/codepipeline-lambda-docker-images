#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

#!/bin/bash
set -eu
set -o pipefail

RESOURCE_PREFIX="${RESOURCE_PREFIX:=stg}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:=us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>&1)
ECR_COMMON_DATALAKE_REPO_URL="${ECR_COMMON_DATALAKE_REPO_URL:=$ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com\/$RESOURCE_PREFIX-common-datalake-library}"
pids=()
pids1=()

PROFILE='new-profile'
aws configure --profile $PROFILE set credential_source EcsContainer

aws --version
$(aws ecr get-login --region $AWS_DEFAULT_REGION --no-include-email)
COMMIT_HASH=$(echo $CODEBUILD_RESOLVED_SOURCE_VERSION | cut -c 1-7)
BUILD_TAG=build-$(echo $CODEBUILD_BUILD_ID | awk -F":" '{print $2}')
IMAGE_TAG=${BUILD_TAG:=COMMIT_HASH:=latest}

cd dockerfiles;
mkdir ../logs
function pwait() {
    while [ $(jobs -p | wc -l) -ge $1 ]; do
        sleep 1
    done
}

function build_dockerfiles() {
    if [ -d $1 ]; then
        directory=$1
        cd $directory
        echo $directory
        echo "---------------------------------------------------------------------------------"
        echo "Start creating docker image for $directory..."
        echo "---------------------------------------------------------------------------------"
            REPOSITORY_URI=$ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$RESOURCE_PREFIX-$directory
            docker build --build-arg ECR_COMMON_DATALAKE_REPO_URL=$ECR_COMMON_DATALAKE_REPO_URL . -t $REPOSITORY_URI:latest -t $REPOSITORY_URI:$IMAGE_TAG -t $REPOSITORY_URI:$COMMIT_HASH
            echo Build completed on `date`
            echo Pushing the Docker images...
            docker push --all-tags $REPOSITORY_URI
        cd ../
        echo "---------------------------------------------------------------------------------"
        echo "End creating docker image for $directory..."
        echo "---------------------------------------------------------------------------------"
    fi
}

for directory in *; do 
   echo "------Started processing code in $directory directory-----"
   build_dockerfiles $directory 2>&1 1>../logs/$directory-logs.log | tee -a ../logs/$directory-logs.log &
   pids+=($!)
   pwait 20
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

cd ../cfn/
function build_cfnpackages() {
    if [ -d ${directory} ]; then
        directory=$1
        cd $directory
        echo $directory
        echo "---------------------------------------------------------------------------------"
        echo "Start packaging cloudformation package for $directory..."
        echo "---------------------------------------------------------------------------------"
        aws cloudformation package --profile $PROFILE --template-file template.yaml --s3-bucket $S3_BUCKET --output-template-file packaged-template.yaml
        echo "Replace the parameter 'pEcrImageTag' value with the latest built tag"
        echo $(jq --arg Image_Tag "$IMAGE_TAG" '.Parameters |= . + {"pEcrImageTag":$Image_Tag}' parameters.json) > parameters.json
        cat parameters.json
        ls -al
        cd ../
        echo "---------------------------------------------------------------------------------"
        echo "End packaging cloudformation package for $directory..."
        echo "---------------------------------------------------------------------------------"
    fi
}

for directory in *; do
    echo "------Started processing code in $directory directory-----"
    build_cfnpackages $directory 2>&1 1>../logs/$directory-logs.log | tee -a ../logs/$directory-logs.log &
    pids1+=($!)
    pwait 20
done

for pid in "${pids1[@]}"; do
  wait "$pid"
done

cd ../logs/
ls -al
for f in *; do
  printf '%s\n' "$f"
  paste /dev/null - < "$f"
done

cd ../