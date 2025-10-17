[
  {
    "name": "e2e-tests",
    "image": "${management_account_id}.dkr.ecr.eu-west-2.amazonaws.com/e2e-tests",
    "portMappings": [],
    "essential": true,
    "environment": [
      {
        "name": "PRESERVICA_API_URL",
        "value": "https://tna.preservica.com"
      },
      {
        "name": "ACCOUNT_ID",
        "value": "${account_id}"
      },
      {
        "name": "PRESERVICA_SECRET_NAME",
        "value": "${secret_name}"
      },
      {
        "name": "ENVIRONMENT",
        "value": "${environment}"
      }
    ],
    "mountPoints": [
      {
        "sourceVolume": "test",
        "containerPath": "/tests/boot",
        "readOnly": false
      },
      {
        "sourceVolume": "tmp",
        "containerPath": "/tmp",
        "readOnly": false
      }
    ],
    "readonlyRootFilesystem": true,
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-create-group": "true",
        "awslogs-group": "/aws/ecs/e2e-tests",
        "awslogs-region": "eu-west-2",
        "awslogs-stream-prefix": "ecs"
      },
      "secretOptions": []
    },
    "family": "e2e-tests",
    "cpu": 1024,
    "memory": 2048,
    "tags": []
  }
]
