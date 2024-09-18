# Test Cases

## 1. `ingested_PS` for 1 item only

This test models the action of adding `ingested_PS` to the Asset after reconciling where the asset is included in the
OPEX package.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | ingested_PS |
|----|---------|---------------|------------|-------------|
| 0  | A       | ArchiveFolder |            |             |
| 1  | A       | Asset         | 0/         | true        |
| 2  | A       | File          | 0/1/       |             |
| 3  | A       | File          | 0/1/       |             |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_PS": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e2",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.923Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

## 2. `skipIngest` for 1 item only

This test models the action of adding `skipIngest` to the Asset after finding the asset is already in the Preservation
System.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | skipIngest |
|----|---------|---------------|------------|------------|
| 0  | A       | ArchiveFolder |            |            |
| 1  | A       | Asset         | 0/         | true       |
| 2  | A       | File          | 0/1/       |            |
| 3  | A       | File          | 0/1/       |            |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "skipIngest": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

No output.

## 3. `ingested_PS`+`skipIngest` for 1 item only

This test models the action of adding `ingested_PS` to the Asset after reconciling where the asset was not included in
the OPEX package because it was already inside the Preservation System.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | skipIngest | ingested_PS |
|----|---------|---------------|------------|------------|-------------|
| 0  | A       | ArchiveFolder |            |            |             |
| 1  | A       | Asset         | 0/         | true       | true        |
| 2  | A       | File          | 0/1/       |            |             |
| 3  | A       | File          | 0/1/       |            |             |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_PS": {
            "BOOL": true
          },
          "skipIngest": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e2",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.923Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

## 4. `ingested_PS`+`skipIngest` for multiple items

This test models the action of adding `ingested_PS` to the Asset after reconciling where the asset was not included in
the OPEX package because it was already inside the Preservation System. But the Asset is not yet in the Custodial Copy
store.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | skipIngest | ingested_PS |
|----|---------|---------------|------------|------------|-------------|
| 0  | A       | ArchiveFolder |            |            |             |
| 1  | A       | Asset         | 0/         |            | true        |
| 2  | A       | File          | 0/1/       |            |             |
| 3  | A       | File          | 0/1/       |            |             |
| 9  | B       | ArchiveFolder |            |            |             |
| 1  | B       | Asset         | 9/         | true       | true        |
| 5  | B       | File          | 9/1/       |            |             |
| 6  | B       | File          | 9/1/       |            |             |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "B"
          }
        },
        "NewImage": {
          "ingested_PS": {
            "BOOL": true
          },
          "skipIngest": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "B"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e2",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.923Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

## 5. `ingested_PS`+`ingested_CC` for 1 Asset only where all files are complete

This test models the action of adding `ingested_PS` or `ingested_CC` to the Asset after reconciling/downloading to
Custodial Copy, where all the child Files are downloaded to Custodial Copy.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | ingested_PS | ingested_CC |
|----|---------|---------------|------------|-------------|-------------|
| 0  | A       | ArchiveFolder |            |             |             |
| 1  | A       | Asset         | 0/         | true        | true        |
| 2  | A       | File          | 0/1/       |             | true        |
| 3  | A       | File          | 0/1/       |             | true        |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_PS": {
            "BOOL": true
          },
          "ingested_CC": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

```json
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

## 6. `ingested_PS`+`ingested_CC` for 1 Asset only where all files are not complete

This test models the action of adding `ingested_PS` or `ingested_CC` to the Asset after reconciling/downloading to
Custodial Copy, where **not** all the child Files are downloaded to Custodial Copy.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | ingested_PS | ingested_CC |
|----|---------|---------------|------------|-------------|-------------|
| 0  | A       | ArchiveFolder |            |             |             |
| 1  | A       | Asset         | 0/         | true        | true        |
| 2  | A       | File          | 0/1/       |             |             |
| 3  | A       | File          | 0/1/       |             | true        |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_PS": {
            "BOOL": true
          },
          "ingested_CC": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```json
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```
## 7. `ingested_CC` for 1 File only where not all files are complete

This test models the action of adding `ingested_CC` to a File after downloading to Custodial Copy, where the Asset is
but **not** all the Files are downloaded to Custodial Copy.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | ingested_PS | ingested_CC |
|----|---------|---------------|------------|-------------|-------------|
| 0  | A       | ArchiveFolder |            |             |             |
| 1  | A       | Asset         | 0/         | true        | true        |
| 2  | A       | File          | 0/1/       |             |             |
| 3  | A       | File          | 0/1/       |             | true        |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "3"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_CC": {
            "BOOL": true
          },
          "id": {
            "S": "3"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "File"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```json
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "3"
  }
}
```

## 8. `ingested_CC` for 1 File only where all files are complete

This test models the action of adding `ingested_CC` to a File after downloading to Custodial Copy, where the Asset and
all the Files are downloaded to Custodial Copy.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | ingested_PS | ingested_CC |
|----|---------|---------------|------------|-------------|-------------|
| 0  | A       | ArchiveFolder |            |             |             |
| 1  | A       | Asset         | 0/         | true        | true        |
| 2  | A       | File          | 0/1/       |             | true        |
| 3  | A       | File          | 0/1/       |             | true        |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "2"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_CC": {
            "BOOL": true
          },
          "id": {
            "S": "2"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "File"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

```json
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

## 9. `ingested_PS`+`ingested_CC` for an Asset where all files are complete, and the asset has been in multiple ingest batches

This test models the action of adding `ingested_PS` or `ingested_CC` to the Asset(s) after reconciling/downloading to
Custodial Copy, where all the child Files are downloaded to Custodial Copy. Since the Asset was first ingested to the
Preservation System, we have tried to ingest it again, adding the `skipIngest` flag and successfully reconciling.

This case only handles the addition of the `ingested_CC` to the Asset in batch A. We will actually run 2 Lambdas in
parallel if this happens which will result in duplicate messages being sent with different `messageId` values - oh well,
idempotency and all that. The assets in each batch could have different `parentMessageId` values which we need to
passthrough, hence 2 messages.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | skipIngest | ingested_PS | ingested_CC |
|----|---------|---------------|------------|------------|-------------|-------------|
| 0  | A       | ArchiveFolder |            |            |             |             |
| 1  | A       | Asset         | 0/         |            | true        | true        |
| 2  | A       | File          | 0/1/       |            |             | true        |
| 3  | A       | File          | 0/1/       |            |             | true        |
| 9  | B       | ArchiveFolder |            |            |             |             |
| 1  | B       | Asset         | 9/         | true       | true        | true        |
| 8  | B       | File          | 9/1/       |            |             |             |
| 7  | B       | File          | 9/1/       |            |             |             |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_PS": {
            "BOOL": true
          },
          "ingested_CC": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e4",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```


## 10. `ingested_CC` for a File where all files are complete, and the asset has been in multiple ingest batches

This test models the action of adding `ingested_CC` to a File after downloading to Custodial Copy, where all the child
Files are downloaded to Custodial Copy. Since the Asset was first ingested to the Preservation System, we have tried to
ingest it again, adding the `skipIngest` flag and successfully reconciling; this subsequent ingest attempt was started
after the IO has already been processed by the CC application.

### State

**dr2-ingest-files table**

| id | batchId | type          | parentPath | skipIngest | ingested_PS | ingested_CC |
|----|---------|---------------|------------|------------|-------------|-------------|
| 0  | A       | ArchiveFolder |            |            |             |             |
| 1  | A       | Asset         | 0/         |            | true        | true        |
| 2  | A       | File          | 0/1/       |            |             | true        |
| 3  | A       | File          | 0/1/       |            |             | true        |
| 9  | B       | ArchiveFolder |            |            |             |             |
| 1  | B       | Asset         | 9/         | true       | true        |             |
| 8  | B       | File          | 9/1/       |            |             |             |
| 7  | B       | File          | 9/1/       |            |             |             |

### Input

```JSON
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "2"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
          "ingested_CC": {
            "BOOL": true
          },
          "id": {
            "S": "2"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "File"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

### Output (messages sent)

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```

```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e4",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.complete"
  },
  "parameters": {
    "assetId": "1"
  }
}
```
```JSON
{
  "properties": {
    "messageId": "e2f8a9a9-4935-4f4f-96f8-7bdfa75960e3",
    "parentMessageId": null,
    "timestamp": "2024-05-23T10:13:16.925Z",
    "type": "preserve.digital.asset.ingest.update"
  },
  "parameters": {
    "assetId": "1"
  }
}
```
