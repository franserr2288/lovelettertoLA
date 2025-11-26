aws cloudformation create-stack \
  --stack-name github-oidc-setup \
  --template-body file://iac/iam/github-oidc-setup.yaml \
  --parameters \
    ParameterKey=GitHubOrg,ParameterValue=franserr2288 \
    ParameterKey=GitHubRepo,ParameterValue=lovelettertoLA \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile local-dev \
  --region us-east-1
 
aws cloudformation deploy \                           
  --stack-name github-oidc-setup \
  --template-file iac/iam/github/github-oidc-setup.yaml \
  --parameter-overrides \
    GitHubOrg=franserr2288 \
    GitHubRepo=lovelettertoLA \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile local-dev \
  --region us-east-1