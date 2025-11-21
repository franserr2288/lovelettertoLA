# set up provider for gh actions
aws cloudformation create-stack \
  --stack-name github-oidc-setup \
  --template-body file://iac/iam/github-oidc-setup.yaml \
  --parameters \
    ParameterKey=GitHubOrg,ParameterValue=franserr2288 \
    ParameterKey=GitHubRepo,ParameterValue=lovelettertoLA \
  --capabilities CAPABILITY_NAMED_IAM