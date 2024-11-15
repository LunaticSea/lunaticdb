return function (req_data, pattern)
  local res = {}
  for _, value in pairs(req_data) do
    local is_match = string.match(value.value, pattern)
    if is_match then table.insert(res, value) end
  end
  return res
end