local json = require('json')
local core = require('class')('core')

-- options.db_name
function core:init(options)
	self.options = options or {}
	self.driver = nil
	self.cache = {}
	self.db_name = options.db_name or 'lunatic_db'
	self.cache[self.db_name] = {}
end

function core:load(driver, config)
	self.driver = driver({
	  db_name = self.db_name,
	  table.unpack(config)
	}, self):load()

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
	assert(self.driver, 'Driver not found, please load it on load()')
	assert(self.cache[self.db_name], "This database doesn't exist")

	local res = self.driver:set(key, data)
	self.cache[self.db_name][key] = res

	return res
end

function core:get(key)
	assert(self.driver, 'Driver not found, please load it on load()')
	assert(self.cache[self.db_name], "This database doesn't exist")

	local cache = self.cache[self.db_name][key]
	local error

	if not cache then
		local driver_get, err = self.driver:get(key)
		cache = driver_get
		error = err
	end

	cache = self:convert_output(cache)
	return cache, error
end

function core:delete(key)
	assert(self.driver, 'Driver not found, please load it on load()')
	assert(self.cache[self.db_name], "This database doesn't exist")

	self.driver:delete(key)
	local deleted_data = self.cache[self.db_name][key]
	self.cache[self.db_name][key] = nil

	return {
		data = deleted_data,
		key = key,
	}
end

function core:delete_all(db_name)
	assert(self.driver, 'Driver not found, please load it on load()')
	db_name = db_name or self.db_name
	self.driver:db_drop(db_name)
	self.cache[db_name] = {}
	return nil
end

function core:all()
	assert(self.driver, 'Driver not found, please load it on load()')
	return self.driver:all()
end

function core:db_drop(db_name)
	assert(self.driver, 'Driver not found, please load it on load()')
	db_name = db_name or self.db_name
	self.driver:db_drop(db_name)
	self.cache[db_name] = nil
	return nil
end

function core:create_db(db_name, driver)
	assert(self.db_name, 'Missing db_name #1 args')

	driver = driver or self.driver
	self.options.db_name = db_name
	
	return core(self.options):load(self.driver, self.driverconfig)
end

function core:convert_input(data)
	if type(data) == 'table' then
		return json.encode(data)
	end
	return tostring(data)
end

function core:convert_output(data)
	if type(data) == 'nil' then return nil end

	local is_json = pcall(json.decode, data)
	if is_json then
		local converted = json.decode(data)
		if converted then return json.decode(data) end
	end

	if data == 'nil' then
		return nil
	end
	if data == 'null' then
		return nil
	end
	if data == 'true' then
		return true
	end
	if data == 'false' then
		return false
	end

	local is_number = not (data == '' or string.match(data, '%D+'))
	if is_number then
		return tonumber(data)
	end

	return data
end

return core