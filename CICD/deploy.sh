#!/bin/bash
set -e
set -o pipefail
export AWS_DEFAULT_REGION=ap-southeast-2
export LAMBDA_FUNCTION="${LAMBDA_FUNCTION:test-lammulti}"
test $LAMBDA_FUNCTION
cd lambda_code
pip install ../../ --target ./
rm -f ../code.zip
zip ../code.zip ./ -r
echo deploying code
aws lambda update-function-code --function-name $LAMBDA_FUNCTION --zip-file fileb://$(pwd)/../code.zip
echo "waiting for lambda to finalize"
aws lambda wait function-updated --function-name $LAMBDA_FUNCTION
echo "invoking $LAMBDA_FUNCTION"
RESULT=$(aws lambda invoke --function-name $LAMBDA_FUNCTION --invocation-type RequestResponse RequestResponse --log-type Tail)
echo $RESULT | jq .
test null = $(echo $RESULT | jq .FunctionError)

