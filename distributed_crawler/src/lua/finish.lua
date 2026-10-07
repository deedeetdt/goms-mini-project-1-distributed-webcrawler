-- KEYS: metadata, frontier, in-flight, seen, extensions, active jobs.
-- ARGV: job ID, parent URL, outcome kind, extension, words, then discoveries.
if redis.call('SISMEMBER', KEYS[3], ARGV[2]) == 0 then
    return 0
end

-- Publish discoveries before removing their parent from flight.
for i = 6, #ARGV do
    if redis.call('SADD', KEYS[4], ARGV[i]) == 1 then
        redis.call('RPUSH', KEYS[2], ARGV[i])
    end
end

if ARGV[3] == 'file' then
    redis.call('HINCRBY', KEYS[1], 'num_files', 1)
    redis.call('HINCRBY', KEYS[1], 'total_word_count', ARGV[5])
    redis.call('HINCRBY', KEYS[5], ARGV[4], 1)
elseif ARGV[3] == 'failed' then
    redis.call('HINCRBY', KEYS[1], 'unsuccessful', 1)
end

redis.call('HINCRBY', KEYS[1], 'processed', 1)
redis.call('SREM', KEYS[3], ARGV[2])

if redis.call('LLEN', KEYS[2]) == 0 and redis.call('SCARD', KEYS[3]) == 0 then
    redis.call('HSET', KEYS[1], 'state', 'done')
    redis.call('SREM', KEYS[6], ARGV[1])
end
return 1
