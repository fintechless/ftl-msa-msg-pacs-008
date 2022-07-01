src = {
  backend    = "s3"
  config_key = "terraform/fintechless/ftl-msa-msg-pacs-008/aws_k8s_deployment/terraform.tfstate"

  msa            = "msg-pacs-008"
  container_port = 5005
}
