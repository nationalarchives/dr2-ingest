variable "lambdas" {
  description = "A map of lambda function name to lambda version"
  type        = map(string)
}

variable "alias_name" {}