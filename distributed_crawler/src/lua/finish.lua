-- KEYS: metadata, frontier, in-flight, seen, extensions, active jobs.
-- ARGV: job ID, parent URL, outcome kind, extension, words, then discoveries.
local metadata = KEYS[1]
local frontier = KEYS[2]
local in_flight = KEYS[3]
local seen = KEYS[4]
local extension_counts = KEYS[5]
local active_jobs = KEYS[6]

local job_id = ARGV[1]
local parent_url = ARGV[2]
local outcome = ARGV[3]
local extension = ARGV[4]
local word_count = ARGV[5]

if redis.call('SISMEMBER', in_flight, parent_url) == 0 then
    return 0
end

-- Save discoveries and results before releasing the parent or checking completion.
-- An empty queue alone is insufficient: an in-flight page may still add URLs.
for i = 6, #ARGV do
    local discovered_url = ARGV[i]
    if redis.call('SADD', seen, discovered_url) == 1 then
        redis.call('RPUSH', frontier, discovered_url)
    end
end

if outcome == 'file' then
    redis.call('HINCRBY', metadata, 'num_files', 1)
    redis.call('HINCRBY', metadata, 'total_word_count', word_count)
    redis.call('HINCRBY', extension_counts, extension, 1)
elseif outcome == 'failed' then
    redis.call('HINCRBY', metadata, 'unsuccessful', 1)
end

redis.call('HINCRBY', metadata, 'processed', 1)
redis.call('SREM', in_flight, parent_url)

if redis.call('LLEN', frontier) == 0 and redis.call('SCARD', in_flight) == 0 then
    redis.call('HSET', metadata, 'state', 'done')
    redis.call('SREM', active_jobs, job_id)
end
return 1
