Feature: Tagging pooled content

  Scenario: I want to tag an item
    Given I have logged in as "bob"
    When I create a new file
    Then I tag the file with a single tag
    Then I tag the file with multiple tags
    Then I delete a tag from the file
    Then Make a bogus tag request
    Then Make a bogus tag delete request

  Scenario: I want to put an item in the directory
    Given I have logged in as "bob"
    When I create a new file
    Then I put the file in the Veterinary directory


