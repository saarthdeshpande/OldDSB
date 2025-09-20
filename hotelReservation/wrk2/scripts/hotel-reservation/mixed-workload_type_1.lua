local socket = require("socket")
local json = require("dkjson")
math.randomseed(socket.gettime() * 1000)
math.random(); math.random(); math.random()

local url = "http://localhost:5000"
local current_method = ""
local current_path = ""
local request_start_time = 0

local request_counts = {}
local latency_buckets = {}
local send_interval = 15
local last_send_time = socket.gettime()
local rps_file_path = ""

-- Unique filename per thread
init = function(args)
  local co = coroutine.running()
  local co_str = tostring(co)
  local thread_id = tonumber(string.match(co_str, "thread: 0x%x+ (%d+)")) or math.random(10000, 99999)
  rps_file_path = string.format("cpu_memory_/rps_%d.txt", thread_id)
end

local function get_user()
  local id = math.random(0, 500)
  local user_name = "Cornell_" .. tostring(id)
  local pass_word = string.rep(tostring(id), 10)
  return user_name, pass_word
end

local function percentile(sorted, p)
  if #sorted == 0 then return 0 end
  local rank = p * (#sorted - 1) + 1
  local lower = math.floor(rank)
  local upper = math.ceil(rank)
  local weight = rank - lower
  if upper > #sorted then return sorted[#sorted] end
  return sorted[lower] * (1 - weight) + sorted[upper] * weight
end

local function flush_rps_file()
  local now = socket.gettime()
  local parts = {}
  local total_rps = 0

  for key, count in pairs(request_counts) do
    local lat_list = latency_buckets[key]
    table.sort(lat_list)
    local rps = count / send_interval
    local p95 = percentile(lat_list, 0.95)
    local p99 = percentile(lat_list, 0.99)
    local avg = 0
    for _, v in ipairs(lat_list) do avg = avg + v end
    avg = avg / #lat_list
    total_rps = total_rps + rps
    table.insert(parts, string.format("%s_rps: %.1f; %s_p95: %.1f; %s_p99: %.1f; %s_avg: %.1f",
      key, rps, key, p95, key, p99, key, avg))
  end

  local f = io.open(rps_file_path, "a")
  if f then
    f:write(string.format("total_rps: %.1f; %s\n", total_rps, table.concat(parts, "; ")))
    f:close()
  end

  request_counts = {}
  latency_buckets = {}
end

-- ENDPOINT DEFINITIONS
local function hotels()
  current_method = "GET"; current_path = "/hotels"
  return wrk.format("GET", url .. "/hotels?inDate=2015-04-15&outDate=2015-04-16&lat=38.0235&lon=-122.095")
end

local function reserve()
  current_method = "POST"; current_path = "/reservation"
  local in_date = math.random(9, 23)
  local out_date = in_date + math.random(1, 5)
  local in_str = string.format("2015-04-%02d", in_date)
  local out_str = string.format("2015-04-%02d", out_date)
  local hotel_id = tostring(math.random(1, 80))
  local user_id, password = get_user()
  local cust_name = user_id
  return wrk.format("POST", url .. "/reservation?inDate=" .. in_str .. "&outDate=" .. out_str ..
    "&lat=38.0235&lon=-122.095&hotelId=" .. hotel_id ..
    "&customerName=" .. cust_name .. "&username=" .. user_id ..
    "&password=" .. password .. "&number=1")
end

local function recommendations()
  current_method = "GET"; current_path = "/recommendations"
  return wrk.format("GET", url .. "/recommendations?require=rate&lat=38.0235&lon=-122.095")
end

-- REQUEST
request = function()
  local coin = math.random()
  request_start_time = socket.gettime()
  if coin < 0.45 then return hotels()
  elseif coin < 0.90 then return recommendations()
  else return reserve() end
end

-- RESPONSE
response = function(status, headers, body)
  local now = socket.gettime()
  local duration_ms = (now - request_start_time) * 1000
  local key = string.format("('%s','%s')", current_path, current_method)

  request_counts[key] = (request_counts[key] or 0) + 1
  latency_buckets[key] = latency_buckets[key] or {}
  table.insert(latency_buckets[key], duration_ms)

  if now - last_send_time >= send_interval then
    flush_rps_file()
    last_send_time = now
  end
end

done = function(summary, latency, requests)
  flush_rps_file()
end
