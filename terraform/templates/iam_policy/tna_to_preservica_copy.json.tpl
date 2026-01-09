{
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${raw_cache_bucket_name}",
        "arn:aws:s3:::${raw_cache_bucket_name}/*",
        "arn:aws:s3:::${adhoc_cache_bucket_name}",
        "arn:aws:s3:::${adhoc_cache_bucket_name}/*"
      ],
      "Sid": "readIngestRawCache"
    },
    {
      "Action": [
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::com.preservica.${preservica_tenant}.bulk1"
      ],
      "Sid": "listBucketPreservica"
    },
    {
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::com.preservica.${preservica_tenant}.bulk1/*",
        "arn:aws:s3:::com.preservica.${preservica_tenant}.bulk2/*",
        "arn:aws:s3:::com.preservica.${preservica_tenant}.bulk3/*",
        "arn:aws:s3:::com.preservica.${preservica_tenant}.bulk4/*",
        "arn:aws:s3:::com.preservica.${preservica_tenant}.put.holding/*"
      ],
      "Sid": "readWriteTnaAndPreservica"
    }
  ],
  "Version": "2012-10-17"
}
