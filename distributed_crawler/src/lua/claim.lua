-- KEYS: job metadata, frontier list, in-flight set.
local state = redis.call('HGET', KEYS[1], 'state')
if not state or state == 'done' then
    return nil
end

local url = redis.call('LPOP', KEYS[2])
if not url then
    return nil
end

redis.call('SADD', KEYS[3], url)
redis.call('HSET', KEYS[1], 'state', 'running')
return url
