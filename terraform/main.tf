locals {
  org  = "pi"
  name = "process-bot"
  env    = ["stag", "prod"]
  region = "us-east-1"

  git_conn_arn = "arn:aws:codestar-connections:us-east-1:724178372978:connection/8b658c00-d2c4-470f-98db-529ca8ff67f3"
  git_org      = "portfolioinsider"
  git_repo     = format("%s/%s", local.git_org, "pi-process-bot")
  git_branch = {
    prod = "main"
    stag = "staging"
  }

  common_tags = {
    region    = local.region
    terraform = true
  }
}

terraform {
  # terraform state storage
  backend "s3" {
    bucket         = "pi-terra-state"
    key            = "apps/pi-process-bot/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "pi-terra-state"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "us-east-1"
}
