terraform {
  backend "s3" {
    bucket               = "ftl-api-deploy-default-us-east-1-123456789012"
    key                  = "terraform/fintechless/ftl-msa-msg-pacs-008/aws_k8s_service/terraform.tfstate"
    region               = "us-east-1"
    workspace_key_prefix = "terraform_workspaces"
  }
}
