-- Simple tax/rounding helpers.

local Tax = {}

local function round_half_up(x, decimals)
  local pow = 10 ^ (decimals or 2)
  return math.floor(x * pow + 0.5) / pow
end

local function round_bankers(x, decimals)
  local pow = 10 ^ (decimals or 2)
  local v = x * pow
  local frac = v - math.floor(v)
  local base = math.floor(v)
  if frac == 0.5 then
    if base % 2 == 0 then
      v = base
    else
      v = base + 1
    end
  else
    v = math.floor(v + 0.5)
  end
  return v / pow
end

function Tax.round(amount, mode, decimals)
  if mode == "bankers" then
    return round_bankers(amount, decimals)
  end
  return round_half_up(amount, decimals)
end

function Tax.total_with_vat(net, vatRate, mode)
  local gross = (net or 0) * (1 + (vatRate or 0))
  return Tax.round(gross, mode or os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2)
end

return Tax
