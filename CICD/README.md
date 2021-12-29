# CICD Pipeline

This folder contains the resources for the CICD pipeline
to do integration testing and unit testing of this library.

`template.yaml` is a CloudFormation template for setting up resources,
manually, to run the integration tests.

`lambda_code` is where we'll construct a zip from to deploy to lambda,
using `deploy.sh`.
