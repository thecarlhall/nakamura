@httpResponse
@user

Given /^I have a user called "([^"]*)"$/ do |username|
  @s.switch_user(SlingUsers::User.admin_user())
  @user = @um.create_user(username + @m)
  @users[username] = @user
end

Given /^I have logged in as "([^"]*)"$/ do |username|
  @user = @users[username]
  @s.switch_user(@user)
end

Given /^I log out$/ do
  @s.switch_user(SlingUsers::User.anonymous)
end

When /^I request the Me feed$/ do
  @httpResponse = @s.execute_get(@s.url_for("/system/me"))
end

Then /^I get the Me feed$/ do
  json = JSON.parse(@httpResponse.body)
  raise "Could not get the feed!" unless @httpResponse.code.to_i == 200
  raise "Expected to find a user property!" unless json['user']
end