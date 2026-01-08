# Rotate preservation system password

This lambda implements the four-step password rotation process for AWS secrets manager:

1. CreateSecret — Creates a new pending secret version, generating a new password if one doesn't already exist.

2. SetSecret — Applies the pending secret to the target system (e.g. resets the password in an external system).

3. TestSecret — Tests the new credentials (e.g. login check).

4. FinishSecret — Promotes the pending version to AWSCURRENT if all previous steps succeeded.

## Lambda input
This lambda is invoked four times for each rotation of a secret. 
```json
{
  "Step": "CreateSecret",
  "SecretId": "id-of-secret",
  "ClientRequestToken": "2f368dda-b913-42c4-9ad5-016b55854dd0"
}
```
```json
{
  "Step": "SetSecret",
  "SecretId": "id-of-secret",
  "ClientRequestToken": "2f368dda-b913-42c4-9ad5-016b55854dd0"
}
```

```json
{
  "Step": "TestSecret",
  "SecretId": "id-of-secret",
  "ClientRequestToken": "2f368dda-b913-42c4-9ad5-016b55854dd0"
}
```

```json
{
  "Step": "FinishSecret",
  "SecretId": "id-of-secret",
  "ClientRequestToken": "2f368dda-b913-42c4-9ad5-016b55854dd0"
}
```

## Step descriptions
The structure of the secret in Secrets Manager is:
```json
{
  "userName": "preservation_user@nationalarchives.gov.uk",
  "password": "thePassword",
  "apiUrl": "https://url.of.preservation.system"
}
```

### Create secret
* Get the existing secret
* Try to get a pending secret. 
* If the pending secret exists, do nothing.
* If it doesn't, generate a new password. 
* Check the password matches the preservation system password rules.
  * Password must be at least eight characters long, and no more than 64 characters long.
  * Password must contain at least three of the following: lowercase letters, uppercase letters, numbers, symbols.
  * Password must not contain a string of characters repeated more than twice, regardless of case
* If the password matches, create a pending version, otherwise generate a new password and repeat the checks.

### Set secret
* Get the current secret
* Get the pending secret
* Use the preservation system API to update the password from the old to the new value.
* If the preservation system rejects this, remove the pending secret.

### TestSecret
* Call a preservation system API with the new secret and make sure it authenticates successfully.

### Finish secret
* Move the 'Current' label from the original secret to the new secret. 