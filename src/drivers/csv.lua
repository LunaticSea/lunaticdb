local fs = require('fs')
local filter = require('../utils/filter')
local spliter = require("../utils/spliter")
local openSync, writeSync, readSync = fs.openSync, fs.writeSync, fs.readSync
local format = string.format

local default = {
  file_name = "lunatic.db.csv"
}

local csv_driver = require('class'):create()

-- options.file_name
-- options.db_name
function csv_driver:init(options, parent)
  options = options or {}
  self.parent = parent
  self.file_name = options.file_name or default.file_name
  self.db_name = options.db_name or 'lunatic_db'
  self.template = {
    withbreak = '"%s", "%s", "%s"\n',
    nobreak = '"%s", "%s", "%s"',
    value_finder = '"%s", "%s", "(.+)"',
    db_finder = '"%s", "(.+)", "(.+)"',
  }
  self.header = "DATABASE, KEY, VALUE\n"
  self.isload = 'not_load'
end

function csv_driver:load()
  self.file = self.file_name and openSync(self.file_name, 'a+')
  self:check_header()
  self.is_load = 'loaded'
  return self
end

function csv_driver:check_header()
  local data, err = readSync(self.file)
  assert(not err, err)
  if not string.match(data, self.header) then
    writeSync(self.file, -1, self.header)
  end
  return true
end

function csv_driver:set(key, data)
  assert(self.file, 'File not avaliable, try change the path or run load() func')
  local exist_data, exist_index = self:get(key)

  data = self.parent:convert_input(data)

  if not exist_data then
    local dump_data = format(self.template.withbreak, self.db_name, key, data)
    writeSync(self.file, -1, dump_data)
    return data
  end

  local new_data = format(self.template.nobreak, self.db_name, key, data)
  if exist_data == data then return data end

  local original_value = self:db_all()
  local new_element = original_value[exist_index]
  new_element.value = new_data
  original_value[exist_index] = new_element

  local string_merge = self:convert_all_output(original_value)
  fs.writeFileSync(self.file_name, string_merge)

  return data
end

function csv_driver:get(key)
  assert(key, 'key not avaliable')
  assert(self.file, 'File not avaliable, try change the path or run load() func')
  local full_data, err = self:db_all()
  assert(not err, err)

  local pattern = string.format(self.template.value_finder, self.db_name, key)

  for _, element in pairs(full_data) do
    local match_data = string.match(element.value, pattern)
    if match_data then return match_data, element.index end
  end

  return nil, nil
end

function csv_driver:delete(key)
  assert(self.file, 'File not avaliable, try change the path or run load() func')
  local exist_data, exist_index = self:get(key)
  if not exist_data or not exist_index then return nil end

  local original_value = self:db_all()
  local new_element = original_value[exist_index]

  new_element.value = nil
  original_value[exist_index] = new_element

  local string_merge = self:convert_all_output(original_value)
  fs.writeFileSync(self.file_name, string_merge)

  return {
    data = exist_data,
    key = key
  }
end

function csv_driver:all(custom_db)
  local res = {}
  local all_data, err = self:db_all()
  assert(not err, err)

  local pattern = format(self.template.db_finder, custom_db or self.db_name)
  local filtered = filter(all_data, pattern)
  for _, element in ipairs(filtered) do
    local element_key, element_value = string.match(element.value, pattern)
    table.insert(res, {
      index = element.index,
      key = element_key,
      data = element_value
    })
  end

  return res
end

function csv_driver:db_all()
  local res = {}
  local data, err = fs.readFileSync(self.file_name)
  if err then return nil, err end
  local splited = spliter(data, '[^\r\n]+')
  for key, value in pairs(splited) do
    table.insert(res, { index = key, value = value })
  end
  return res, nil
end

function csv_driver:db_drop(db_name)
  assert(self.file, 'File not avaliable, try change the path or run load() func')
  local original_value = self:db_all()

  local pattern = format(self.template.db_finder, db_name)

  for index, element in pairs(original_value) do
    local is_match = string.match(element.value, pattern)
    if not is_match then
      local new_element = original_value[element.index]
      new_element.value = nil
      original_value[index] = new_element
    end
  end

  local string_merge = self:convert_all_output(original_value)
  fs.writeFileSync(self.file_name, string_merge)
end

function csv_driver:db_create(db_name)
  assert(self.file, 'File not avaliable, try change the path or run load() func')
  return csv_driver:new({ db_name = db_name })
end

function csv_driver:convert_all_output(obj_data)
  local new_string = ''
  for _, element in pairs(obj_data) do
    if type(element.value) ~= 'nil' then
      new_string = new_string .. element.value .. '\n'
    end
  end
  return new_string
end

return csv_driver