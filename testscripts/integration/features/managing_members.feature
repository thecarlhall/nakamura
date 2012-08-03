Feature: Managing members in pooled content

Scenario: I want to set the viewer of a pooled content item
  Given I have logged in as "bob"
  When I create a new file
  When I grant "alice" permission to view the file
  Then "alice" has read only privileges

  Given I have logged in as "alice"
  Then User cannot delete the file
  And User cannot see acl
  And User cannot write the file
  And User can read file

  Given I have logged in as "bob"
  When I revoke "alice" permission to view the file
  Then "alice" has no privileges

  Given I have logged in as "alice"
  And User cannot see acl
  And User cannot read file

Scenario: I want to set the editor of a pooled content item
  Given I have logged in as "bob"
  When I create a new file
  When I grant "alice" permission to edit the file
  Then "alice" has read and write privileges

  Given I have logged in as "alice"
  Then User can write the file
  And User cannot delete the file
  And User cannot see acl

  Given I have logged in as "bob"
  When I revoke "alice" permission to edit the file
  Then "alice" has no privileges

  Given I have logged in as "alice"
  And User cannot see acl
  And User cannot read file

Scenario: I want to set the manager of a pooled content item
  Given I have logged in as "bob"
  When I create a new file
  When I grant "alice" permission to manage the file
  Then "alice" has read write and delete privileges

  Given I have logged in as "alice"
  And User can write the file
  And User can see acl
  And User can delete the file

  Given I have logged in as "bob"
  When I create a new file
  When I grant "alice" permission to manage the file
  Then "alice" has read write and delete privileges
  When I revoke "alice" permission to manage the file
  Then "alice" has no privileges

  Given I have logged in as "alice"
  And User cannot see acl
  And User cannot read file

Scenario: I want to see who has permissions on a pooled content item
  Given I have logged in as "bob"
  When I create a new file
  And I grant "carol" permission to view the file
  And I grant "ted" permission to edit the file
  And I grant "alice" permission to manage the file

  Then I see the correct list of viewers, editors, and managers on the file

Scenario: A non-manager tries to manage an item
  Given I have logged in as "bob"
  When I create a new file
  And I grant "ted" permission to edit the file
  And I grant "carol" permission to view the file

  Given I have logged in as "ted"
  Then Make sure user can not manage an item without manage permission

