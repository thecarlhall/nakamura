@httpResponse
@fm
@filejson
@user
@fileurl
@fileinfinityurl
@poolid

When /^I create a new file$/ do
  @httpResponse = @fm.upload_pooled_file("file1", "file contents", "text/plain")
  if @httpResponse.code.to_i == 201
    @filejson = JSON.parse(@httpResponse.body)
    @poolid = @filejson["file1"]["poolId"]
    @fileurl = @s.url_for("/p/#{@poolid}")
    @fileinfinityurl = @s.url_for("/p/#{@poolid}.infinity.json")
    @log.info(@filejson)
  else
    @filejson = nil
    @fileurl = nil
    @poolid = nil
  end
end

When /^I create a new piece of pooled content$/ do
  @httpResponse = @s.execute_post(@s.url_for("/system/pool/createfile"), {"prop" => "val"})
  if @httpResponse.code.to_i == 201
    @filejson = JSON.parse(@httpResponse.body)
    @poolid = @filejson["_contentItem"]["poolId"]
    @fileurl = @s.url_for("/p/#{@poolid}")
    @fileinfinityurl = @s.url_for("/p/#{@poolid}.infinity.json")
    @log.info(@filejson)
  else
    @filejson = nil
    @fileurl = nil
  end
end

Then /^I check the properties of the new file$/ do
  raise "Could not create the file!" unless @httpResponse.code.to_i == 201
  item = @filejson["file1"]["item"]
  raise "File id was not returned" unless @poolid != nil
  raise "File's mimetype is incorrect!" unless item["_mimeType"] == "text/plain"
  raise "sakai:pooled-content-file-name did not get set" unless item["sakai:pooled-content-file-name"] == "file1"
  raise "sling:resourceType is incorrect" unless item["sling:resourceType"] == "sakai/pooled-content"
  raise "sakai:pool-content-created-for should be the test user" unless item["sakai:pool-content-created-for"] == @user.name
  raise "sakai:pooled-content-manager should be the test user" unless item["sakai:pooled-content-manager"][0] == @user.name
  raise "sakai:needsprocessing was not set to true" unless item["sakai:needsprocessing"] == "true"
end

Then /^I check the properties of the new pooled content/ do
  raise "Could not create the file!" unless @httpResponse.code.to_i == 201
  item = @filejson["_contentItem"]["item"]
  raise "File id was not returned" unless @poolid != nil
  raise "sling:resourceType is incorrect" unless item["sling:resourceType"] == "sakai/pooled-content"
  raise "sakai:pool-content-created-for should be the test user" unless item["sakai:pool-content-created-for"] == @user.name
  raise "sakai:pooled-content-manager should be the test user" unless item["sakai:pooled-content-manager"][0] == @user.name
end

Then /^I check the json feed of the new file$/ do
  filejson = @s.execute_get(@fileinfinityurl)
  raise "Failed to get poolid.infinity.json" unless filejson.code.to_i == 200
end

Then /^I check the body content of the new file$/ do
  filebodyhttpresponse = @s.execute_get(@fileurl)
  raise "Could not fetch file content!" unless filebodyhttpresponse.code.to_i == 200
  raise "File content is incorrect" unless filebodyhttpresponse.body == "file contents"
end

Then /^I change the body content$/ do
  filemodificationpost = @fm.upload_pooled_file("file1", "modified contents", "text/plain", @poolid)
  raise "File modification post failed!" unless filemodificationpost.code.to_i == 200
  filebodyhttpresponse = @s.execute_get(@fileurl)
  raise "File content is incorrect" unless filebodyhttpresponse.body == "modified contents"
end

Then /^Anonymous user cannot post content$/ do
  raise "Anonymous user should not be able to post content" unless @httpResponse.code.to_i == 405
end

Then /^I create an alternative stream of a file$/ do
  altstreampost = @fm.upload_pooled_file("file1", "page1 large contents", "text/plain", "#{@poolid}.page1-large")
  raise "Alternative stream of file could not be created" unless altstreampost.code.to_i == 200
end

Then /^I get the alternative stream$/ do
  altstreamget = @s.execute_get(@s.url_for("/p/#{@poolid}/page1.large.txt"))
  raise "Alternative stream of file could not be fetched" unless altstreamget.code.to_i == 200
  raise "Alternative stream content is incorrect" unless altstreamget.body == "page1 large contents"
  altstreampropsget = @s.execute_get(@s.url_for("/p/#{@poolid}/page1.json"))
  raise "Alternative stream of file could not be fetched" unless altstreampropsget.code.to_i == 200
  json = JSON.parse(altstreampropsget.body)
  raise "Alternative stream does not have proper resource type" unless json["sling:resourceType"] == "sakai/pooled-content"
end