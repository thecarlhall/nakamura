When /^I grant "([^"]*)" permission to view the file$/ do |user|
  username = user + @m
  manageresponse = @fm.manage_members(@poolid, [username], [], [], [], [], [])
  raise "Setting viewer permission failed" unless manageresponse.code.to_i == 200
end

When /^I revoke "([^"]*)" permission to view the file$/ do |user|
  username = user + @m
  manageresponse = @fm.manage_members(@poolid, [], [username], [], [], [], [])
  raise "Revoking viewer permission failed" unless manageresponse.code.to_i == 200
end

When /^I grant "([^"]*)" permission to edit the file$/ do |user|
  username = user + @m
  manageresponse = @fm.manage_members(@poolid, [], [], [], [], [username], [])
  raise "Setting editor permission failed" unless manageresponse.code.to_i == 200
end

When /^I revoke "([^"]*)" permission to edit the file$/ do |user|
  username = user + @m
  manageresponse = @fm.manage_members(@poolid, [], [], [], [], [], [username])
  raise "Revoking editor permission failed" unless manageresponse.code.to_i == 200
end

When /^I grant "([^"]*)" permission to manage the file$/ do |user|
  username = user + @m
  manageresponse = @fm.manage_members(@poolid, [], [], [username], [], [], [])
  raise "Setting manager permission failed" unless manageresponse.code.to_i == 200
end

When /^I revoke "([^"]*)" permission to manage the file$/ do |user|
  username = user + @m
  manageresponse = @fm.manage_members(@poolid, [], [], [], [username], [], [])
  raise "Revoking manager permission failed" unless manageresponse.code.to_i == 200
end

Then /^"([^"]*)" has no privileges$/ do |user|
  username = user + @m
  aclget = @s.execute_get(@s.url_for("/p/#{@poolid}.acl.json"))
  @log.debug(aclget.body)
  json = JSON.parse(aclget.body)
  raise "#{username}'s permissions are incorrect" unless json[username]["granted"].empty?
end

Then /^"([^"]*)" has read only privileges$/ do |user|
  username = user + @m
  aclget = @s.execute_get(@s.url_for("/p/#{@poolid}.acl.json"))
  @log.debug(aclget.body)
  json = JSON.parse(aclget.body)
  raise "#{username}'s permissions are incorrect" unless json[username]["granted"].include?("Read") && json[username]["granted"].length == 1
end

Then /^"([^"]*)" has read and write privileges$/ do |user|
  username = user + @m
  aclget = @s.execute_get(@s.url_for("/p/#{@poolid}.acl.json"))
  @log.debug(aclget.body)
  json = JSON.parse(aclget.body)
  raise "#{username}'s permissions are incorrect" unless json[username]["granted"].include?("Read") && json[username]["granted"].include?("Write")
end

Then /^"([^"]*)" has read write and delete privileges$/ do |user|
  username = user + @m
  aclget = @s.execute_get(@s.url_for("/p/#{@poolid}.acl.json"))
  @log.debug(aclget.body)
  json = JSON.parse(aclget.body)
  raise "#{username}'s permissions are incorrect" unless json[username]["granted"].include?("All")
end

Then /^User can read file$/ do
  filebodyhttpresponse = @s.execute_get(@fileurl)
  raise "Other user should have access to content!" unless filebodyhttpresponse.code.to_i == 200
end

Then /^User cannot read file$/ do
  filebodyhttpresponse = @s.execute_get(@fileurl)
  raise "Other user should not have access to content!" unless filebodyhttpresponse.code.to_i == 404
end

Then /^User can write the file$/ do
  writepost = @s.execute_post(@fileurl, "otheruserprop" => "value")
  raise "User should be able to write to file" unless writepost.code.to_i == 200

  get = @s.execute_get(@fileinfinityurl)
  json = JSON.parse(get.body)
  raise "User's edit did not persist" unless json["otheruserprop"] == "value"
end

Then /^User cannot write the file$/ do
  writepost = @s.execute_post(@fileurl, "otheruserprop" => "value")
  raise "User should not be able to write to file" unless writepost.code.to_i == 403
end

Then /^User can delete the file$/ do
  delete = @s.execute_post(@fileurl, ":operation" => "delete")
  @log.info(delete.body)
  raise "User should be able to delete file" unless delete.code.to_i == 200
end

Then /^User cannot delete the file$/ do
  delete = @s.execute_post(@fileurl, ":operation" => "delete")
  raise "User should not be able to delete file" unless delete.code.to_i == 403
end

Then /^User can see acl$/ do
  aclget = @s.execute_get(@s.url_for("/p/#{@poolid}.acl.json"))
  raise "User should be able to see acl" unless aclget.code.to_i == 200
end

Then /^User cannot see acl$/ do
  aclget = @s.execute_get(@s.url_for("/p/#{@poolid}.acl.json"))
  raise "User should not be able to see acl" unless aclget.code.to_i == 404
end

Then /^I see the correct list of viewers, editors, and managers on the file$/ do
  memberget = @s.execute_get(@s.url_for("/p/#{@poolid}.members.json"))
  @log.debug(memberget.body)
  json = JSON.parse(memberget.body)
  raise "There should be 2 managers" unless json["managers"].length == 2
  raise "Bob should be a manager" unless json["managers"][0]["hash"] == @bob.name || json["managers"][1]["hash"] == @bob.name
  raise "Alice should be a manager" unless json["managers"][0]["hash"] == @alice.name || json["managers"][1]["hash"] == @alice.name
  raise "Ted should be an editor" unless json["editors"].length == 1 && json["editors"][0]["hash"] == @ted.name
  raise "Carol should be a viewer" unless json["viewers"].length == 1 && json["viewers"][0]["hash"] == @carol.name

  detailedmemberget = @s.execute_get(@s.url_for("/p/#{@poolid}.members.detailed.tidy.json"))
  raise "Detailed view of members did not return successfully" unless detailedmemberget.code.to_i == 200
  json = JSON.parse(detailedmemberget.body)
  raise "Detailed view of members should contain last name from profile" unless json["managers"][0]["basic"]["elements"]["lastName"] != nil
end

Then /^Make sure user can not manage an item without manage permission$/ do
  manageresponse = @fm.manage_members(@poolid, ["anonymous"], [], [], [], [], [])
  raise "Non-manager should not be able to add anonymous as viewer" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, ["everyone"], [], [], [], [], [])
  raise "Non-manager should not be able to add everyone as viewer" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, [], [], [], [], ["anonymous"], [])
  raise "Non-manager should not be able to add anonymous as editor" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, [], [], [], [], ["everyone"], [])
  raise "Non-manager should not be able to add everyone as editor" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, [], [], ["anonymous"], [], [], [])
  raise "Non-manager should not be able to add anonymous as manager" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, [], [], ["everyone"], [], [], [])
  raise "Non-manager should not be able to add everyone as manager" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, [], [], [], [@bob.name], [], [])
  raise "Non-manager should not be able to remove manager" unless manageresponse.code.to_i == 403

  manageresponse = @fm.manage_members(@poolid, [], [@carol.name], [], [], [], [])
  raise "Non-manager should not be able to remove viewer" unless manageresponse.code.to_i == 403

end
