Feature: Commenting on pooled content

Scenario: I want to comment on a file
  Given I have logged in as "alice"
  When I create a new file
  When I grant everyone permission to read the file
  Then A brand-new file has no comments on it
  Then Make a bogus comment post
  Then Comment on the file
  Then Check that the comment was posted
  Then Edit an existing comment
  Then Delete an existing comment
  Then Comment on the file

Scenario: Somebody else wants to comment on my file
  Given I have logged in as "bob"
  When I create a new file
  When I grant everyone permission to read the file

  Given I have logged in as "alice"
  Then Comment on the file

  Given I have logged in as "carol"
  Then Edit an existing comment as a non-managing viewer
  Then Delete an existing comment without the rights to do so

  When I grant everyone permission to manage the file

  Given I have logged in as "ted"
  Then Edit an existing comment as a manager
  Then Delete an existing comment


