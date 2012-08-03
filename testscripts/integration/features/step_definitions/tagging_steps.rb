Then /^Make a bogus tag request$/ do
  tagpost = @s.execute_post(@fileurl, ":operation" => "tag")
  raise "Tag op without 'key' should fail" unless tagpost.code.to_i == 400
end

Then /^Make a bogus tag delete request$/ do
  tagpost = @s.execute_post(@fileurl, ":operation" => "deletetag")
  raise "Delete Tag op without 'key' should fail" unless tagpost.code.to_i == 400

  tagpost = @s.execute_post(@fileurl, ":operation" => "deletetag", "key" => "nonexistent" + @m)
  raise "Delete Tag with nonexistent tag key should fail" unless tagpost.code.to_i == 404

  tagpost = @s.execute_post(@fileurl, ":operation" => "deletetag", "key" => @poolid)
  raise "Passing DeleteTagOperation a key that is not a tag should fail" unless tagpost.code.to_i == 400
end

Then /^I tag the file with a single tag$/ do
  tagpath = "/tags/foo" + @m
  tagpost = @s.execute_post(@fileurl, ":operation" => "tag", "key" => tagpath)
  raise "Single tag could not be applied to file" unless tagpost.code.to_i == 200

  tagget = @s.execute_get(@s.url_for(tagpath))
  raise "Tag could not be retrieved" unless tagget.code.to_i == 200
  json = JSON.parse(tagget.body)
  raise "Tag resource type is incorrect" unless json["sling:resourceType"] == "sakai/tag"
  raise "Tag name is incorrect" unless json["sakai:tag-name"] == "foo" + @m
  raise "Tag count is incorrect" unless json["sakai:tag-count"] == 1

  fileget = @s.execute_get(@fileinfinityurl)
  json = JSON.parse(fileget.body)
  raise "Tag does not appear in file data" unless json["sakai:tags"][0] == "foo" + @m
end

Then /^I tag the file with multiple tags$/ do
  firsttag = "/tags/first" + @m
  secondtag = "/tags/second" + @m
  tagpost = @s.execute_post(@fileurl, ":operation" => "tag", "key" => [firsttag, secondtag])
  raise "Multiple tags could not be applied to file" unless tagpost.code.to_i == 200

  tagget = @s.execute_get(@s.url_for(firsttag))
  raise "First Tag could not be retrieved" unless tagget.code.to_i == 200
  json = JSON.parse(tagget.body)
  raise "First Tag resource type is incorrect" unless json["sling:resourceType"] == "sakai/tag"
  raise "First Tag name is incorrect" unless json["sakai:tag-name"] == "first" + @m
  raise "First Tag count is incorrect" unless json["sakai:tag-count"] == 1

  tagget = @s.execute_get(@s.url_for(secondtag))
  raise "Second Tag could not be retrieved" unless tagget.code.to_i == 200
  json = JSON.parse(tagget.body)
  raise "Second Tag resource type is incorrect" unless json["sling:resourceType"] == "sakai/tag"
  raise "Second Tag name is incorrect" unless json["sakai:tag-name"] == "second" + @m
  raise "Second Tag count is incorrect" unless json["sakai:tag-count"] == 1

  fileget = @s.execute_get(@fileinfinityurl)
  json = JSON.parse(fileget.body)
  raise "First Tag does not appear in file data" unless json["sakai:tags"].include?("first" + @m)
  raise "Second Tag does not appear in file data" unless json["sakai:tags"].include?("second" + @m)

end

Then /^I delete a tag from the file$/ do
  tagpath = "/tags/foo" + @m
  tagpost = @s.execute_post(@fileurl, ":operation" => "deletetag", "key" => tagpath)
  raise "Delete tag operation failed" unless tagpost.code.to_i == 200

  fileget = @s.execute_get(@fileinfinityurl)
  json = JSON.parse(fileget.body)
  raise "Deleted Tag should not appear in file data" if json["sakai:tags"].include?("foo" + @m)
end

Then /^I put the file in the Veterinary directory$/ do
  tagpath = "/tags/directory/Veterinary" + @m
  tagpost = @s.execute_post(@fileurl, ":operation" => "tag", "key" => tagpath)
  raise "Directory tag could not be applied to file" unless tagpost.code.to_i == 200

  wait_for_indexer()

  directoryget = @s.execute_get(@s.url_for("/tags/directory.tagged.tidy.json"))
  raise "Could not get directory.tagged.json feed" unless directoryget.code.to_i == 200
  json = JSON.parse(directoryget.body)
  @log.info(directoryget.body)
  listing = json["Veterinary" + @m]
  raise "Veterinary#{@m} does not appear in directory" unless listing != nil
  raise "Content item does not appear in Veterinary#{@m} listing" unless listing["content"]["sakai:tags"].include?("directory/Veterinary" + @m)
end