local helpers = require "spec.helpers"

describe("kong config", function()
  local yaml_path = "spec/fixtures/declarative_config.yaml"

  local db

  lazy_setup(function()
    local _
    _, db = helpers.get_db_utils(nil, {
      "plugins", "routes", "services"
    }) -- runs migrations
    helpers.prepare_prefix()
  end)
  after_each(function()
    helpers.kill_all()
  end)
  before_each(function()
    db.plugins:truncate()
    db.routes:truncate()
    db.services:truncate()
  end)
  lazy_teardown(function()
    helpers.clean_prefix()
  end)

  it("config help", function()
    local _, stderr = helpers.kong_exec "config --help"
    assert.not_equal("", stderr)
  end)

  it("config imports a yaml file", function()
    assert(helpers.kong_exec("start", {
      prefix = helpers.test_conf.prefix,
      database = helpers.test_conf.database,
      pg_database = helpers.test_conf.pg_database,
      cassandra_keyspace = helpers.test_conf.cassandra_keyspace
    }))

    -- FIXME `Error: no configuration schema found for plugin: error-generator-post` on next line
    assert(helpers.kong_exec("config import " .. yaml_path, {
      prefix = helpers.test_conf.prefix,
    }))

    assert(helpers.start_kong())
    local client = helpers.admin_client()

    local res = client:get("/services/foo")
    assert.res_status(200, res)
  end)
end)
