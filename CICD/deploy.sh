#!/bin/bash
set -e
set -o pipefail
test $LAMBDA_FUNCTION
cd lambda_code
pip install ../../ --target ./
rm -f ../code.zip
zip ../code.zip ./ -r
echo deploying code
aws lambda update-function-code --function-name $LAMBDA_FUNCTION --zip-file fileb://$(pwd)/../code.zip
echo "waiting for lambda to finalize"
aws lambda wait function-updated --function-name $LAMBDA_FUNCTION
echo "invoking"
RESULT=$(aws lambda invoke --function-name $LAMBDA_FUNCTION --invocation-type RequestResponse RequestResponse --log-type Tail)
echo $RESULT | jq .
test null = $(echo $RESULT | jq .FunctionError)

