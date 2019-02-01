local helpers = require "spec.helpers"

local fmod    = math.fmod


local function is_valid_page(assert, rows, err, err_t)
  assert.is_nil(err_t)
  assert.is_nil(err)
  assert.is_table(rows)
end

for _, strategy in helpers.each_strategy() do
  describe("kong.db [#" .. strategy .. "]", function()
    local db, bp

    lazy_setup(function()
      bp, db = helpers.get_db_utils(strategy, {
        "services",
      })
    end)

    -- Note by default the page size is 100, we should keep this number
    -- less than 100/(tags_per_entity)
    local test_entity_count = 10

    local removed_tags_count = 0

    it("insert tags with entity", function()
      for i = 1, test_entity_count do
        local service = {
          host = "example-" .. i .. ".com",
          name = "service" .. i,
          tags = { "team_a", "level_"..fmod(i, 5), "service"..i }
        }
        local row, err, err_t = bp.services:insert(service)
        assert.is_nil(err)
        assert.is_nil(err_t)
        assert.same(service.tags, row.tags)
      end
    end)

    it("list all entities attached with tag", function()
      local rows, err, err_t, offset = db.tags:page()
      is_valid_page(assert, rows, err, err_t)
      assert.is_nil(offset)
      assert.equal(test_entity_count*3, #rows)
      for _, row in ipairs(rows) do
        assert.equal("services", row.entity_name)
      end
    end)

    it("list entity IDs by tag", function()
      local rows, err, err_t, offset = db.tags:page_by_tag("team_a")
      is_valid_page(assert, rows, err, err_t)
      assert.is_nil(offset)
      assert.equal(test_entity_count, #rows)
      for _, row in ipairs(rows) do
        assert.equal("team_a", row.tag)
      end

      rows, err, err_t, offset = db.tags:page_by_tag("team_alien")
      is_valid_page(assert, rows, err, err_t)
      assert.is_nil(offset)
      assert.equal(0, #rows)

      rows, err, err_t, offset = db.tags:page_by_tag("service1")
      is_valid_page(assert, rows, err, err_t)
      assert.is_nil(offset)
      assert.equal(1, #rows)
      for _, row in ipairs(rows) do
        assert.equal("service1", row.tag)
      end
      
    end)


    describe("update row in tags table with", function()
      local service1 = db.services:select_by_name("service1")
      assert.is_not_nil(service1)
      assert.is_not_nil(service1.id)

      local service3 = db.services:select_by_name("service3")
      assert.is_not_nil(service3)
      assert.is_not_nil(service3.id)

      -- due to the different sql in postgres stragey
      -- we need to test these two methods seperately
      local scenarios = {
        { "update", { id = service1.id }, "service1", }, 
        { "update_by_name", "service2", "service2"},
        { "upsert", { id = service3.id }, "service3" }, 
        { "upsert_by_name", "service4", "service4"},
      }
      for _, scenario in pairs(scenarios) do
        local func, key, removed_tag = unpack(scenario)

        it(func, function()
          local tags = { "team_b_" .. func, "team_a" }
          local row, err, err_t = db.services[func](db.services,
          key, { tags = tags, host = 'whatever.com' })

          assert.is_nil(err)
          assert.is_nil(err_t)
          for _, tag in ipairs(tags) do
            assert.contains(tag, row.tags)
          end

          removed_tags_count = removed_tags_count + 1

          local rows, err, err_t, offset = db.tags:page()
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(test_entity_count*3 - removed_tags_count, #rows)

          rows, err, err_t, offset = db.tags:page_by_tag("team_a")
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(test_entity_count, #rows)

          rows, err, err_t, offset = db.tags:page_by_tag("team_b_" .. func)
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(1, #rows)

          rows, err, err_t, offset = db.tags:page_by_tag(removed_tag)
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(0, #rows)
        end)

      end
    end)

    describe("delete row in tags table with", function()
      local service5 = db.services:select_by_name("service5")
      assert.is_not_nil(service5)
      assert.is_not_nil(service5.id)

      -- due to the different sql in postgres stragey
      -- we need to test these two methods seperately
      local scenarios = {
        { "delete", { id = service5.id }, "service5" }, 
        { "delete_by_name", "service6", "service6" },
      }
      for i, scenario in pairs(scenarios) do
        local delete_func, delete_key, removed_tag = unpack(scenario)
    
        it(delete_func, function()
          local ok, err, err_t = db.services[delete_func](db.services, delete_key)
          assert.is_true(ok)
          assert.is_nil(err)
          assert.is_nil(err_t)

          removed_tags_count = removed_tags_count + 3

          local rows, err, err_t, offset = db.tags:page()
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(test_entity_count*3 - removed_tags_count, #rows)

          rows, err, err_t, offset = db.tags:page_by_tag("team_a")
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(test_entity_count - i, #rows)

          rows, err, err_t, offset = db.tags:page_by_tag(removed_tag)
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(0, #rows)
        end)

      end
    end)

    describe("insert row in tags table with", function()
      -- due to the different sql in postgres stragey
      -- we need to test these two methods seperately
      local scenarios = {
        { "upsert", { id = require("kong.tools.utils").uuid() }, { "service-upsert-1" } }, 
        { "upsert_by_name", "service-upsert-2", { "service-upsert-2" } },
      }
      for _, scenario in pairs(scenarios) do
        local func, key, tags = unpack(scenario)

        it(func, function()
          local row, err, err_t = db.services[func](db.services,
          key, { tags = tags, host = 'whatever.com' })

          assert.is_nil(err)
          assert.is_nil(err_t)
          for _, tag in ipairs(tags) do
            assert.contains(tag, row.tags)
          end

          local rows, err, err_t, offset = db.tags:page_by_tag(tags[1])
          is_valid_page(assert, rows, err, err_t)
          assert.is_nil(offset)
          assert.equal(1, #rows)
        end)

      end
    end)


    describe("page() by tag", function()
      local single_tag_count = 5
      local total_entities_count = 100
      for i = 1, total_entities_count do
        local service = {
          host = "anotherexmaple-" .. i .. ".org",
          name = "service-paging" .. i,
          tags = { "paging", "team_paging_" .. fmod(i, 5), "irrelevant_tag" }
        }
        local row, err, err_t = bp.services:insert(service)
        assert.is_nil(err)
        assert.is_nil(err_t)
        assert.same(service.tags, row.tags)
      end

      local scenarios = { -- { tags[], expected_result_count }
        {
          { { "paging" } },
          total_entities_count,
        },
        {
          { { "paging", "team_paging_1" }, "or" },
          total_entities_count,
        },
        {
          { { "team_paging_1", "team_paging_2" }, "or" },
          total_entities_count/single_tag_count*2,
        }, 
        {
          { { "paging", "team_paging_1" }, "and" },
          total_entities_count/single_tag_count,
        },
        {
          { { "team_paging_1", "team_paging_2" }, "and" },
          0,
        },
      }

      local paging_size = { total_entities_count/single_tag_count, }

      for s_idx, scenario in ipairs(scenarios) do
        local opts, expected_count = unpack(scenario)
        for i=1, 2 do -- also produce a size=nil iteration
          local size = paging_size[i]

          local scenario_name = string.format("#%d %s %s", s_idx, opts[2] and opts[2]:upper() or "",
                                              size and "with pagination" or "")
          
          --  page() #1 condition pagination  results count is expected

          describe(scenario_name, function()
            local seen_entities = {}
            local seen_entities_count = 0

            it("results don't overlap", function()
              local rows, err, err_t, offset
              while true do
                rows, err, err_t, offset = db.services:page(size, offset,
                  { tags = opts[1], tags_cond = opts[2] }
                )
                is_valid_page(assert, rows, err, err_t)
                for _, row in ipairs(rows) do
                  assert.is_nil(seen_entities[row.id])
                  seen_entities[row.id] = true
                  seen_entities_count = seen_entities_count + 1
                end
                if not offset then
                  break
                end
              end

            end)

            it("results count is expected", function()
              assert.equal(expected_count, seen_entities_count)
            end)
          end)
        end

      end

      local func = pending
      if strategy == "cassandra" then
        func = describe
      end
  
      func("limits maximum queries in single request", function()
        local match = require("luassert.match")
        -- Might be flaky because it depends on how cassandra partition/order row
        it("and exits early if PAGING_MAX_QUERY_ROUNDS exceeded", function()
          stub(ngx, "log")
          
          local rows, err, err_t, offset = db.services:page(2, nil, 
            { tags = { "paging", "tag_notexist" }, tags_cond = 'and' })
          is_valid_page(assert, rows, err, err_t)
          assert.is_not_nil(offset)
          -- actually #rows will be 0 in this certain test case,
          -- but put as < 2(size) as it's what logically expected
          assert.is_true(#rows < 2)
  
          assert.stub(ngx.log).was_called()
          assert.stub(ngx.log).was_called_with(ngx.WARN, match.is_same("maximum "),  match.is_same(20),
                                        match.is_same(" rounds exceeded "),
                                        match.is_same("without retrieving required size of rows, "),
                                        match.is_same("consider lower the sparsity of tags, or increase the paging size per request"))
        end)
  
        local enough_page_size = total_entities_count/single_tag_count
        it("and doens't throw warning if page size is large enough", function()
          stub(ngx, "log")
          
          local rows, err, err_t, offset = db.services:page(enough_page_size, nil,
            { tags = { "paging", "tag_notexist" }, tags_cond = 'and' })
          is_valid_page(assert, rows, err, err_t)
          assert.equal(0, #rows)
          assert.is_nil(offset)
  
          assert.stub(ngx.log).was_not_called()
        end)
      
        it("and returns as normal if page size is large enough", function()
          stub(ngx, "log")
          
          local rows, err, err_t, offset = db.services:page(enough_page_size, nil,
          { tags = { "paging", "team_paging_1" }, tags_cond = 'and' })
          is_valid_page(assert, rows, err, err_t)
          assert.equal(enough_page_size, #rows)
          if offset then
            rows, err, err_t, offset = db.services:page(enough_page_size, offset,
            { tags = { "paging", "team_paging_1" }, tags_cond = 'and' })
            is_valid_page(assert, rows, err, err_t)
            assert.equal(0, #rows)
            assert.is_nil(offset)
          end
  
          assert.stub(ngx.log).was_not_called()
        end)
      end)

    end)


    local func = pending
    if strategy == "postgres" then
      func = describe
    end
    func("trigger defined for all core entities", function()
      -- TODO: test basic insert on other entities on tags table
      -- to avoid typo
    end)
  end)
end