Feature: Ingest tests

  Scenario Outline: Ingest should succeed if all metadata is valid
    Given An ingest with <count> files for "<source>" source system
    When I send a message to the "<source>" importer queue
    Then I receive an ingest complete message
    Examples:
      | count | source   |
      | 50    | TDR      |
      | 10    | Adhoc    |

  Scenario: Judgment should succeed if all metadata is valid
    Given A judgment
    When I send a message to the "Judgment" importer queue
    Then I receive an ingest complete message

  Scenario: Ingest should fail if there is an empty checksum
    Given An ingest with 1 file with an empty checksum
    When I create a batch with this file
    Then I receive an ingest error message

  Scenario: Ingest should fail if there is an invalid checksum
    Given An ingest with 1 file with an invalid checksum
    When I create a batch with this file
    Then I receive an ingest error message

  Scenario Outline: Ingest should fail if there is invalid metadata
    Given An ingest with <count> file with invalid metadata for "<source>" source system
    When I send a message to the "<source>" importer queue
    Then I receive an error in the "<source>" validation queue
    
    Examples:
      | count | source   |
      | 1     | TDR      |
      | 1     | Adhoc    |
      | 1     | DRI      |
    