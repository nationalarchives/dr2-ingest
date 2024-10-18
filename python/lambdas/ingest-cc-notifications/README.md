# DR2 Ingest CC Notification Handler

This project is for a lambda that is triggered by a notification, sent via our Custodial copy.

1. The Lambda receives the JSON String event and retrieves the JSON message from it
2. It gets the value that belongs to `tableItemIdentifier`
3. It calls `get_item_with_id` with the `id` key and the `tableItemIdentifier` value to get the item that 
   corresponds to that id
4. it then calls `add_true_to_ingest_cc_attribute` which takes the `id` and `batchId` of the item and then writes 
   `true` to the `ingest_CC` attribute on the item. If the `ingest_CC` attribute doesn't already exist, it will add 
   it (give it the value of `true`)

# Troubleshooting
   When it comes to the tests, avoid using `import boto3` and instead do `from boto3 import ...` because something in
   boto3 seems to interfere with the moto library and cause errors

## Environment Variables

| Name                           | Description                 |
|--------------------------------|-----------------------------|
| FILES_DDB_TABLE                | The name of the files table |


