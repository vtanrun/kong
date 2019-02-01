local tablex = require "pl.tablex"

local deepcopy     = tablex.deepcopy
local table_insert = table.insert

local DeclarativeConfig = {}

-- Given a dict of schemas, return an array where if a schema B has a foreign key to A,
-- then B appears after A
local sort_schemas_topologically
do

  -- given a list of schemas, build a table were the keys are schemas and the values are
  -- the list of schemas with foreign keys pointing to them
  --
  -- Example: given { services, routes, plugins, consumers } it will return:
  --
  -- { [services] = { routes, plugins },
  --   [routes] = { plugins },
  --   [consumers] = { plugins }
  -- }
  local function build_neighbors_map(schemas)
    local res = {}
    local destination
    for _, source in pairs(schemas) do
      for _, field in source:each_field() do
        if field.type == "foreign"  then
          destination = schemas[field.schema.name] -- services
          if destination then
            res[destination] = res[destination] or {}
            table_insert(res[destination], source)
          end
        end
      end
    end

    return res
  end


  local function visit(current, neighbors_map, visited, sorted)
    visited[current] = true

    local schemas_pointing_to_current = neighbors_map[current]
    if schemas_pointing_to_current then
      local neighbor
      for i = 1, #schemas_pointing_to_current do
        neighbor = schemas_pointing_to_current[i]
        if not visited[neighbor] then
          visit(neighbor, neighbors_map, visited, sorted)
        end
      end
    end

    table_insert(sorted, 1, current.name)
  end


  sort_schemas_topologically = function(schemas)
    local sorted = {}
    local visited = {}
    local neighbors_map = build_neighbors_map(schemas)

    for _, current in pairs(schemas) do
      if not visited[current] then
        visit(current, neighbors_map, visited, sorted)
      end
    end

    return sorted
  end
end


function DeclarativeConfig.import(db, dc_table)
  assert(type(dc_table) == "table")

  local schemas = {}
  for entity_name, _ in pairs(dc_table) do
    schemas[entity_name] = db[entity_name].schema
  end
  local sorted_schemas = sort_schemas_topologically(schemas)

  for i = 1, #sorted_schemas do
    local schema_name = sorted_schemas[i]
    for _, attributes in pairs(dc_table[schema_name]) do
      attributes = deepcopy(attributes)
      attributes._tags = nil

      local ok, err, err_t = db[schema_name]:insert(attributes)
      if not ok then
        return nil, err, err_t
      end
    end
  end

  return true
end


return DeclarativeConfig

