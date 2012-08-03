Capybara.app_host = "http://localhost:8080"
Capybara.default_driver = :selenium

include SlingInterface
include SlingUsers

Before do
  @s = Sling.new()
  @um = UserManager.new(@s)
  @fm = SlingFile::FileManager.new(@s)
  @log = Logger.new(STDOUT)
  @m = Time.now.to_f.to_s.gsub(".", "")

  # users
  @bob = @um.create_user("bob" + @m)
  @carol = @um.create_user("carol" + @m)
  @ted = @um.create_user("ted" + @m)
  @alice = @um.create_user("alice" + @m)
  @users = {
      "bob" => @bob,
      "carol" => @carol,
      "ted" => @ted,
      "alice" => @alice
  }

  # set log level if an environment var is passed in. Values from Logger::Severity:
  # 0-5. 0 = debug, 5 = unknown.
  if ENV["NAKAMURA_TEST_LOG_LEVEL"] != nil
    @log.level = ENV["NAKAMURA_TEST_LOG_LEVEL"].to_i
  else
    @log.level = Logger::ERROR
  end

  def uniqueness()
    Time.now.to_f.to_s.gsub(".", "")
  end

  ## TODO copied from nakamura-ruby/lib/nakamura/test.rb to avoid issue with transitive deps (assert is undefined).
  ## Figure out if we can use the library version instead.
  def wait_for_indexer()
    magic_content = "#{uniqueness()}_#{rand(1000).to_s}"
    current_user = @s.get_user()
    path = "~#{current_user.name}/private/wait_for_indexer_#{magic_content}"
    urlpath = @s.url_for(path)
    res = @s.execute_post(urlpath, {
        "sling:resourceType" => "sakai/widget-data",
        "sakai:indexed-fields" => "foo",
        "foo" => magic_content
    })
    #assert(res.code.to_i >= 200 && res.code.to_i < 300, "Expected to be able to create node #{res.body}")
    # Give the indexer up to 20 seconds to catch up, but stop waiting early if
    # we find our test item has landed in the index.
    20.times do
      res = @s.execute_get(@s.url_for("/var/search/pool/all.json?q=#{magic_content}"))
      if JSON.parse(res.body)["total"] != 0
        break
      end
      sleep(1)
    end
    res = @s.execute_post(urlpath, {
        ":operation" => "delete"
    })
    #assert(res.code.to_i >= 200 && res.code.to_i < 300, "Expected to be able to delete node #{res.body}")
  end

end