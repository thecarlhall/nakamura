Feature: Me Service

Scenario: I want my me
  Given I have logged in as "alice"
  When I request the Me feed
  Then I get the Me feed

