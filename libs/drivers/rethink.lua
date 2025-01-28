local f = string.format
local Rethink = require('class')('RethinkDriver')
local rethink = require('rethink-luvit')

--[[
local lunaticdb = require('lunaticdb')
local csv_driver = lunaticdb.driver.rethink

local db = lunaticdb.core({ db_name = value })
db:load(driver, {
  host = '127.0.0.1',
  port = 28015,
  username = 'admin',
  password = '',
  database = 'test',
  logLevel = 3,
})

db:set('Hi', 'Hello')
print(db:get('Hi'))
]]

local default = {
  host = '127.0.0.1',
  port = 28015,
  username = 'admin',
  password = '',
  database = 'lunatic_db',
  logLevel = 3,
}

function Rethink:init(options, parent)
  options = options or {}
  options.database = options.db_name
	self.options = options or {}
  for key, value in pairs(default) do
    if not options[key] then
      options[key] = value
    end
  end
	self.parent = parent
  self.db_name = options.database
  self.connection = nil
  self.current_db = nil
  self.error = ''
end

function Rethink:load()
  self.connection = rethink.Connection.new(self.options)

  local handler_close_event = function ()
    self.connection:removeListener('connectionClosed', handler_close_event)
    self.connection = nil
    self:load()
  end

  self.connection:on('connectionClosed', handler_close_event)

  local success = self.connection:connect()
  if not success then return self:load() end

  self.connection.r.table_create(self.db_name):run()

  self.current_db = self.connection.r.table(self.db_name)

	return self
end

function Rethink:set(key, data)
  assert(self.current_db, 'Something went wrong')

  local success_insert
  if self:get(key) then
    success_insert = self.current_db.update(
      { id = key, data = data }
    ):run()
  else
    success_insert = self.current_db.insert(
      { id = key, data = data }
    ):run()
  end

  if not success_insert then return nil end

  return data
end

function Rethink:get(key)
  assert(self.current_db, 'Something went wrong')

  local success_get, cursor = self.current_db.get(key):run()

  if not success_get then return nil end

  if type(cursor[1]) == "table" and #cursor[1] == 0 then
    return nil
  else
    return cursor[1]
  end
end

function Rethink:delete(key)
  assert(self.current_db, 'Something went wrong')

  local exist_data, exist_index = self:get(key)

	if not exist_data or not exist_index then
		return nil
	end

  local success_delete = self.current_db.delete(key):run()

  if not success_delete then return nil end

  return {
		data = exist_data.data,
		key = key,
	}
end

function Rethink:all(custom_db)
  local res = {}

  local target_db = self.connection.r.table(custom_db or self.db_name)

  local success_all, cursor = target_db:run()
	if not success_all then return nil end

  for key, value in pairs(cursor) do
    table.insert(res, {
			index = key,
			key = value.id,
			data = value.data,
		})
  end

  return res
end

function Rethink:db_all()
  return self:all()
end

function Rethink:db_drop(db_name)
  local target_db = self.connection.r.table(db_name or self.db_name)

  local success_all, cursor = target_db:run()
	if not success_all then return nil end

  for key, _ in pairs(cursor) do
    target_db.delete(key):run()
  end
end

function Rethink:db_create(db_name)
  local modded_option = self.options
  modded_option.database = db_name
  return Rethink(db_name)
end

return Rethink
