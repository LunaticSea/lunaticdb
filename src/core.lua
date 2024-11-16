local json = require('json')
local core = require('class'):create()
local default_csv = require('./drivers/csv.lua')

-- options.db_name
function core:init(options)
  self.options = options or {}
  self.driver = nil
  self.cache = {}
  self.db_name = options.db_name or 'lunatic_db'
  self.cache[self.db_name] = {}
end

-- options.driver
-- options[other_args]
function core:load(options)
  if options and options.driver then
    assert(options.driver.is_load ~= 'loaded', "Driver must be unload before use load() function")
  end
  self.driver_options = options or {
    driver = default_csv
  }
  self.driver_options.db_name = self.db_name
  self.driver = self.driver_options.driver:new(self.driver_options, self):load()
  self:_load_cache()

  return self
end

function core:_load_cache()
  local full_db = self.driver:all()
  for _, elements in pairs(full_db) do
    self.cache[self.db_name][elements.key] = elements.data
  end
end

function core:set(key, data)
  assert(self.driver, "Driver not found, please load it on load()")
  assert(self.cache[self.db_name], "This database doesn't exist")

  local res = self.driver:set(key, data)
  self.cache[self.db_name][key] = res

  return res
end

function core:get(key)
  assert(self.driver, "Driver not found, please load it on load()")
  assert(self.cache[self.db_name], "This database doesn't exist")

  local cache = self.cache[self.db_name][key]
  if cache then return cache end

  local driver_get, err = self.driver:get(key)
  return driver_get, err
end

function core:delete(key)
  assert(self.driver, "Driver not found, please load it on load()")
  assert(self.cache[self.db_name], "This database doesn't exist")

  self.driver:delete(key)
  local deleted_data = self.cache[self.db_name][key]
  self.cache[self.db_name][key] = nil

  return {
    data = deleted_data,
    key = key
  }
end

function core:delete_all(db_name)
  assert(self.driver, "Driver not found, please load it on load()")
  db_name = db_name or self.db_name
  self.driver:db_drop(db_name)
  self.cache[db_name] = {}
  return nil
end

function core:all()
  assert(self.driver, "Driver not found, please load it on load()")
  return self.driver:all()
end

function core:db_drop(db_name)
  assert(self.driver, "Driver not found, please load it on load()")
  db_name = db_name or self.db_name
  self.driver:db_drop(db_name)
  self.cache[db_name] = nil
  return nil
end

function core:create_db(db_name, driver)
  assert(self.db_name, "Missing db_name #1 args")
  self.options.db_name = db_name
  if driver then self.driver_options = driver end
  return core:new(self.options):load()
end

function core:convert_input(data)
  if type(data) == 'table' then return json.encode(data) end
  return tostring(data)
end

function core:convert_output(data)
  local is_json = pcall(json.decode, data)
  if type(is_json) == 'table' then return is_json end

  if data == 'nil' then return nil end
  if data == 'null' then return nil end
  if data == 'true' then return true end
  if data == 'false' then return false end

  local is_number = not (data == "" or string.match(data, '%D+'))
  if is_number then return tonumber(data) end

  return nil
end

return core