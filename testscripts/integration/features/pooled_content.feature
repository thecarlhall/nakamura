Feature: Creating and modifying pooled content

Scenario: I want to create and modify a file
  And I have logged in as "ted"
  When I create a new file
  Then I check the properties of the new file
  Then I check the body content of the new file
  Then I check the json feed of the new file
  Then I change the body content

Scenario: As an anonymous user, I want to create content
  Given I log out
  When I create a new file
  Then Anonymous user cannot post content

Scenario: I want to verify that new content is private to me
  Given I have logged in as "alice"
  When I create a new file
  Then I check the properties of the new file
  Then I log out
  Then User cannot read file
  Given I have logged in as "bob"
  Then User cannot read file

Scenario: I want to create a piece of pooled content
  Given I have logged in as "carol"
  When I create a new piece of pooled content
  Then I check the properties of the new pooled content

Scenario: I want to create a file with alternative streams
  Given I have logged in as "carol"
  When I create a new file
  Then I create an alternative stream of a file
  Then I get the alternative stream