"""IAM roles and permissions — pseudo-code.

IAM roles are created for:
1. Lambda execution: shared by all Lambdas, with permissions to read/write
    the relevant S3 prefixes, and write logs to CloudWatch.

2. Glue service: used by the ETL job and crawler, with permissions to read/write
    the relevant S3 prefixes, manage Glue Catalog tables, and write logs.

In production these would be defined via AWS CDK.
"""

ACCOUNT_ID = "ACCOUNT_ID"
BUCKET_ARN = "arn:aws:s3:::bluealpha-data"

IAM_ROLES = {
    # Follows least-privilege: 
    # each role gets only the permissions it needs scoped to specific S3 prefixes and AWS resources.
}

S3_BUCKET_POLICY = {
    # The bucket policy restricts access to the Lambda and Glue roles only.
    # No public access. SSL required for all requests.
}
