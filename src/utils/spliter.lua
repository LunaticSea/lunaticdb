return function(string, pattern)
	local t = {}
	for i in string.gmatch(string, pattern) do
		t[#t + 1] = i
	end
	return t
end
