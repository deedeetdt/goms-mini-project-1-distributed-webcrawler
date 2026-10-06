-- KEYS: submissions hash, job metadata, seen set, frontier list, active set.
-- ARGV: candidate job ID, normalized base URL.
local existing = redis.call('HGET', KEYS[1], ARGV[2])
if existing then
    return existing
end

redis.call('HSET', KEYS[2],
    'base_url', ARGV[2], 'state', 'waiting',
    'num_files', 0, 'total_word_count', 0, 'processed', 0, 'unsuccessful', 0)
redis.call('SADD', KEYS[3], ARGV[2])
redis.call('RPUSH', KEYS[4], ARGV[2])
redis.call('SADD', KEYS[5], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[2], ARGV[1])
return ARGV[1]
