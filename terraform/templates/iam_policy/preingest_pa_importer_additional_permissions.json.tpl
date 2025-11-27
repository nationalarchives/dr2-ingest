{
  "Statement": [
    {
      "Action": [
        "sts:AssumeRole"
      ],
      "Effect": "Allow",
      "Resource": "${pa_migration_role}",
      "Sid": "assumePAMigrationRole"
    }
  ],
  "Version": "2012-10-17"
}