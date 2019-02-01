local helpers = require "spec.helpers"
local cjson = require "cjson"

-- We already test the functionality of page() when filtering by tag in 
-- spec/02-integration/03-db/07-tags_spec.lua.
-- This test we test on the correctness of the admin API response so that
-- we can ensure the the right function (page()) is executed.
describe("Admin API - Kong routes", function()
  for _, strategy in helpers.each_strategy() do
    describe("/entities?tags= with DB: #" .. strategy, function()
      local client, bp

      lazy_setup(function()
        bp = helpers.get_db_utils(strategy, {
          "consumers",
        })

        assert(helpers.start_kong {
          database = strategy,
        })
        client = helpers.admin_client(10000)

        for i = 1, 2 do
          local consumer = {
            username = "adminapi-filter-by-tag-" .. i,
            tags = { "corp_a",  "consumer"..i }
          }
          local row, err, err_t = bp.consumers:insert(consumer)
          assert.is_nil(err)
          assert.is_nil(err_t)
          assert.same(consumer.tags, row.tags)
        end
      end)

      lazy_teardown(function()
        if client then client:close() end
        helpers.stop_kong()
      end)

      it("filter by single tag", function()
        local res = assert(client:send {
          method = "GET",
          path = "/consumers?tags=corp_a"
        })
        local body = assert.res_status(200, res)
        local json = cjson.decode(body)
        assert.equals(2, #json.data)
        for i = 1, 2 do
          assert.contains('corp_a', json.data[i].tags)
        end
      end)

      it("filter by multiple tags with AND", function()
        local res = assert(client:send {
          method = "GET",
          path = "/consumers?tags=corp_a,consumer1"
        })
        local body = assert.res_status(200, res)
        local json = cjson.decode(body)
        assert.equals(1, #json.data)
        assert.equals(2, #json.data[1].tags)
        assert.contains('corp_a', json.data[1].tags)
        assert.contains('consumer1', json.data[1].tags)
      end)

      it("filter by multiple tags with OR", function()
        local res = assert(client:send {
          method = "GET",
          path = "/consumers?tags=consumer2/consumer1"
        })
        local body = assert.res_status(200, res)
        local json = cjson.decode(body)
        assert.equals(2, #json.data)
      end)

      it("returns the correct 'next' arg", function()
        local tags_arg = 'tags=corp_a'
        local res = assert(client:send {
          method = "GET",
          path = "/consumers?" .. tags_arg .. "&size=1"
        })
        local body = assert.res_status(200, res)
        local json = cjson.decode(body)
        assert.equals(1, #json.data)
        assert.match(tags_arg, json.next)
      end)

    end)

  end
end)
