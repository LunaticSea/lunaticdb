  return {
    name = "lunaticdb",
    version = "0.0.1",
    description = "A simple description of my little package.",
    tags = { "lua", "lit", "luvit" },
    license = "AGPL-v3.0",
    author = { name = "RainyXeon", email = "xeondev@xeondex.onmicrosoft.com" },
    homepage = "https://github.com/LunaticSea/lunaticdb",
    dependencies = {
      "luvit/require@2.2.3",
      "luvit/json@v2.5.2",
      "luvit/core@v2.0.4"
    },
    files = {
      "**.lua",
      "!test*"
    }
  }
  