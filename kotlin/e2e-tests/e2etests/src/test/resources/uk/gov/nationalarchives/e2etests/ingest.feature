Feature: Ingest tests

  Scenario: Ingest should succeed if all metadata is valid
    Given An ingest with 1 files
    When I send messages to the input queue
    Then I receive an ingest complete message

  Scenario: Ingest should fail if there is an empty checksum
    Given An ingest with 1 file with an empty checksum
    When I create a batch with this file
    Then I receive an ingest error message

  Scenario: Ingest should fail if there is an invalid checksum
    Given An ingest with 1 file with an invalid checksum
    When I create a batch with this file
    Then I receive an ingest error message

  Scenario: Ingest should fail if there is invalid TDR metadata
    Given An ingest with 1 file with invalid metadata
    When I send messages to the input queue
    Then I receive an error in the validation queue