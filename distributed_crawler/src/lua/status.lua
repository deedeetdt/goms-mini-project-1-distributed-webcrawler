-- KEYS: job metadata, frontier list, in-flight set.
if redis.call('EXISTS', KEYS[1]) == 0 then
    return nil
end

return {
    redis.call('HGET', KEYS[1], 'base_url'),
    redis.call('HGET', KEYS[1], 'state'),
    redis.call('HGET', KEYS[1], 'num_files'),
    redis.call('HGET', KEYS[1], 'processed'),
    redis.call('HGET', KEYS[1], 'unsuccessful'),
    redis.call('LLEN', KEYS[2]),
    redis.call('SCARD', KEYS[3])
}
