-- Lightweight JSON Schema validator for write-side commands.
-- Uses embedded schemas loaded from schemas/*.json (envelope + actions).

local Schema = {}

local has_cjson, cjson = pcall(require, "cjson")
local function simple_decode(str)
  local pos = 1
  local function skip_ws()
    local _, np = str:find("^[ \n\r\t]*", pos)
    pos = (np or pos - 1) + 1
  end
  local function parse_value()
    skip_ws()
    local ch = str:sub(pos, pos)
    if ch == '"' then
      pos = pos + 1
      local start = pos
      while pos <= #str do
        local c = str:sub(pos, pos)
        if c == '"' then
          local val = str:sub(start, pos - 1)
          pos = pos + 1
          return val
        end
        pos = pos + 1
      end
    elseif ch == "{" then
      pos = pos + 1
      local obj = {}
      skip_ws()
      if str:sub(pos, pos) == "}" then pos = pos + 1; return obj end
      while true do
        skip_ws()
        local key = parse_value()
        skip_ws()
        if str:sub(pos, pos) ~= ":" then return nil end
        pos = pos + 1
        local val = parse_value()
        obj[key] = val
        skip_ws()
        local sep = str:sub(pos, pos)
        pos = pos + 1
        if sep == "}" then break end
        if sep ~= "," then return nil end
      end
      return obj
    elseif ch == "[" then
      pos = pos + 1
      local arr = {}
      skip_ws()
      if str:sub(pos, pos) == "]" then pos = pos + 1; return arr end
      while true do
        local val = parse_value()
        table.insert(arr, val)
        skip_ws()
        local sep = str:sub(pos, pos)
        pos = pos + 1
        if sep == "]" then break end
        if sep ~= "," then return nil end
      end
      return arr
    else
      local lit = str:match("^[%w%.%-]+", pos)
      if not lit then return nil end
      pos = pos + #lit
      if lit == "true" then return true end
      if lit == "false" then return false end
      if lit == "null" then return nil end
      local num = tonumber(lit)
      return num
    end
  end
  local ok, val = pcall(parse_value)
  if not ok then return nil end
  return val
end

local function decode_json(str)
  if has_cjson then
    return cjson.decode(str)
  end
  local ok, dkjson = pcall(require, "dkjson")
  if ok then
    local obj, pos, err = dkjson.decode(str, 1, nil)
    if err then return nil end
    return obj
  end
  return simple_decode(str)
end

local function read_json(path)
  local f = io.open(path, "r")
  if not f then return nil end
  local content = f:read("*a")
  f:close()
  return decode_json(content)
end

local ROOT = (... and (...):match("^(.*)%.schema$")) or ""
local envelope_path = "schemas/command-envelope.schema.json"
local actions_path = "schemas/actions.schema.json"

local ENVELOPE = read_json(envelope_path) or {}
local ACTIONS = read_json(actions_path) or {}

local function type_of(value)
  local t = type(value)
  if t == "table" then
    local i = 0
    for _ in pairs(value) do
      i = i + 1
      if value[i] == nil then
        return "object"
      end
    end
    return "array"
  end
  return t
end

local function validate_properties(value, schema, path, errors)
  if schema.required then
    for _, req in ipairs(schema.required) do
      if value[req] == nil then
        table.insert(errors, path .. req .. " is required")
      end
    end
  end
  if schema.properties then
    for name, prop in pairs(schema.properties) do
      local v = value[name]
      if v ~= nil then
        local actual_type = type_of(v)
        if prop.type and actual_type ~= prop.type then
          table.insert(errors, path .. name .. " expected " .. prop.type .. ", got " .. actual_type)
        end
        if prop.enum then
          local ok_enum = false
          for _, ev in ipairs(prop.enum) do
            if ev == v then ok_enum = true end
          end
          if not ok_enum then
            table.insert(errors, path .. name .. " not in enum")
          end
        end
        if prop.pattern and actual_type == "string" then
          if not tostring(v):match(prop.pattern) then
            table.insert(errors, path .. name .. " does not match pattern")
          end
        end
        if prop.minLength and actual_type == "string" and #tostring(v) < prop.minLength then
          table.insert(errors, path .. name .. " shorter than minLength")
        end
        if prop.maxLength and actual_type == "string" and #tostring(v) > prop.maxLength then
          table.insert(errors, path .. name .. " longer than maxLength")
        end
        if prop.type == "array" and prop.items and type(v) == "table" then
          for idx, item in ipairs(v) do
            local itype = type_of(item)
            if prop.items.type and itype ~= prop.items.type then
              table.insert(errors, path .. name .. "[" .. idx .. "] expected " .. prop.items.type .. ", got " .. itype)
            end
            if prop.items.pattern and itype == "string" and not tostring(item):match(prop.items.pattern) then
              table.insert(errors, path .. name .. "[" .. idx .. "] pattern mismatch")
            end
          end
          if prop.minItems and #v < prop.minItems then
            table.insert(errors, path .. name .. " fewer than minItems")
          end
        elseif prop.type == "object" and prop.properties and type(v) == "table" then
          validate_properties(v, prop, path .. name .. ".", errors)
        end
        if prop.format == "date-time" and actual_type == "string" then
          if not tostring(v):match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$") then
            table.insert(errors, path .. name .. " invalid date-time")
          end
        end
      end
    end
  end
  if schema.additionalProperties == false then
    for k in pairs(value) do
      if not (schema.properties and schema.properties[k]) then
        table.insert(errors, path .. k .. " is not allowed")
      end
    end
  end
end

local function validate(value, schema)
  local errors = {}
  validate_properties(value, schema, "", errors)
  return #errors == 0, errors
end

function Schema.validate_envelope(envelope)
  return validate(envelope, ENVELOPE)
end

function Schema.validate_action(action, payload)
  local action_schema = ACTIONS.properties and ACTIONS.properties[action]
  if not action_schema then
    return false, { "action not supported" }
  end
  return validate(payload or {}, action_schema)
end

Schema._debug = {
  envelope = ENVELOPE,
  actions = ACTIONS,
}

return Schema
