@allcomments
@commentid

When /^I grant everyone permission to read the file$/ do
  olduser = @s.get_user()
  @s.switch_user(SlingUsers::User.admin_user())
  @fm.manage_members(@poolid, [ "everyone" ], [], [], [])
  @s.switch_user(olduser)
end

When /^I grant everyone permission to manage the file$/ do
  olduser = @s.get_user()
  @s.switch_user(SlingUsers::User.admin_user())
  @fm.manage_members(@poolid, ["everyone"], [], [ "everyone" ], [])
  @s.switch_user(olduser)
end

Then /^A brand-new file has no comments on it$/ do
  commenturl = @s.url_for("/p/#{@poolid}.comments")
  commentget = @s.execute_get(commenturl)
  @allcomments = JSON.parse(commentget.body)
  raise "A new file should not have any comments" unless @allcomments[0] == nil
end

Then /^Make a bogus comment post$/ do
  posturl = @s.url_for("/p/#{@poolid}.comments")
  commentpost = @s.execute_post(posturl, { "foo" => "bar"})
  raise "Comment should have posted successfully" unless commentpost.code.to_i == 400
end

Then /^Comment on the file$/ do
  posturl = @s.url_for("/p/#{@poolid}.comments")
  commentpost = @s.execute_post(posturl, { "comment" => "something witty",
                                           "prop1" => "KERN-1536 allow arbitrary properties to be stored on a comment"})
  raise "Comment should have posted successfully" unless commentpost.code.to_i == 201
  comments = JSON.parse(commentpost.body)
  raise "Comment ID should have come back to us" unless comments["commentId"] != nil
  @commentid = comments["commentId"]
end

Then /^Check that the comment was posted$/ do
  commenturl = @s.url_for("/p/#{@poolid}.comments")
  commentget = @s.execute_get(commenturl)
  @allcomments = JSON.parse(commentget.body)
  comment = @allcomments["comments"][0]
  @commentid = comment["commentId"]
  raise "The comment body is not present" unless comment["comment"] == "something witty"
  raise "The user hash was not recorded" unless comment["hash"] == @user.name
  raise "The comment author's profile is not present" unless comment["basic"]["elements"]["lastName"] != nil
  commentnodeget = @s.execute_get(@s.url_for("/p/#{@commentid}"))
  @log.info("Comment ID = #{@commentid}")
  commentnodejson = JSON.parse(commentnodeget.body)
  raise "prop1 should have been stored" unless commentnodejson["prop1"] == "KERN-1536 allow arbitrary properties to be stored on a comment"
  verify_comment_count(1)
end

Then /^Edit an existing comment$/ do
  posturl = @s.url_for("/p/#{@poolid}.comments")
  commentpost = @s.execute_post(posturl, { "comment" => "modified witty", "commentId" => @commentid})
  raise "Edit the existing comment failed" unless commentpost.code.to_i == 200
  commenturl = @s.url_for("/p/#{@poolid}.comments")
  commentget = @s.execute_get(commenturl)
  json = JSON.parse(commentget.body)
  raise "The comment body is not present" unless json["comments"][0]["comment"] == "modified witty"
  verify_comment_count(1)
end

Then /^Edit an existing comment as a non-managing viewer/ do
  posturl = @s.url_for("/p/#{@poolid}.comments")
  commentpost = @s.execute_post(posturl, { "comment" => "alice's witty rejoinder", "commentId" => @commentid})
  raise "Edit the existing comment as a non-managing user should not be possible" unless commentpost.code.to_i == 403
end

Then /^Edit an existing comment as a manager$/ do
  posturl = @s.url_for("/p/#{@poolid}.comments")
  @log.info(posturl)
  @log.info(@commentid)
  @log.info(@s)
  @log.info(@user)
  commentpost = @s.execute_post(posturl, { "comment" => "alice's managerial rejoinder", "commentId" => @commentid})
  raise "Edit the existing comment as manager should be possible" unless commentpost.code.to_i == 200
end

Then /^Delete an existing comment$/ do
  id = @commentid.split("/")[-1]
  deleteresponse = @s.delete_file(@s.url_for("/p/#{@poolid}.comments"), { "commentId" => id })
  raise "Delete comment should have returned HTTP No Content" unless deleteresponse.code.to_i == 204
  verify_comment_count(0)
end

Then /^Delete an existing comment without the rights to do so$/ do
  id = @commentid.split("/")[-1]
  deleteresponse = @s.delete_file(@s.url_for("/p/#{@poolid}.comments"), { "commentId" => id })
  raise "Non-manager should not be able to delete content" unless deleteresponse.code.to_i == 403
end

def verify_comment_count(count)
  contentget = @s.execute_get(@fileinfinityurl)
  contentjson = JSON.parse(contentget.body)
  @log.info("commentCount = #{contentjson["commentCount"]}")
  if count > 0
    raise "The comment count should still be #{count}" unless contentjson["commentCount"] == count
  end
end