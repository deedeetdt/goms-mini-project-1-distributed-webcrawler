-- KEYS: job metadata and extension counts.
if redis.call('EXISTS', KEYS[1]) == 0 then
    return nil
end

if redis.call('HGET', KEYS[1], 'state') ~= 'done' then
    -- Do not expose partial counts. The client only uses this unfinished flag.
    return {0, 0, 0, {}}
end

return {
    1,
    redis.call('HGET', KEYS[1], 'num_files'),
    redis.call('HGET', KEYS[1], 'total_word_count'),
    redis.call('HGETALL', KEYS[2])
}
