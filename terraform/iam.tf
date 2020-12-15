
# codepipeline role
resource "aws_iam_role" "process" {
  name               = format("codepipeline-%s-%s", local.org, local.name)
  assume_role_policy = data.aws_iam_policy_document.fetch_sts.json
  tags               = local.common_tags
}
data "aws_iam_policy_document" "fetch_sts" {
  statement {
    effect = "Allow"
    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "Service"
      identifiers = ["codepipeline.amazonaws.com"]
    }
  }
}
resource "aws_iam_role_policy" "process" {
  name = format("codepipeline-policy-%s-%s", local.org, local.name)
  role = aws_iam_role.process.id

  policy = data.aws_iam_policy_document.process.json
}
resource "aws_iam_role_policy_attachment" "process" {
  role       = aws_iam_role.process.name
  policy_arn = "arn:aws:iam::aws:policy/AWSElasticBeanstalkFullAccess"
}
data "aws_iam_policy_document" "process" {
  statement {
    effect = "Allow"
    actions = [
      "codestar-connections:UseConnection",
    ]

    resources = [
      local.git_conn_arn
    ]
  }
}
