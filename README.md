# Entity Event Generator
This is a Lambda which will call the Preservica API to get the entities that have been updated
since a specified date and then publish a message for each to SNS and update DynamoDB with the date
of the last action (performed on an entity) as long as that date is before the date that the event
was triggered.
