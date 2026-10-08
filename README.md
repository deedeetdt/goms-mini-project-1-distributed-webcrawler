# Mini Project 1: Distributed Webcrawler

An async Rust crawler driven by one command-line binary, `crawl`. Each submitted
URL creates an independent job. One or more host processes fetch the job's
reachable URLs and coordinate through a shared Redis instance running in Docker.

Each node runs up to 10 Tokio workers. The CLI supports submission, progress
snapshots, following progress, and final file, extension, and word statistics.
Nodes keep waiting for new jobs after earlier jobs complete.

## Requirements

- Rust and Cargo; Rust 1.91.0 is the tested toolchain.
- Docker with its engine running, and the Redis 7 image.
- Network access from every node to Redis and the websites being crawled.

The examples below use a macOS/Linux shell. Installed `crawl` commands also work
on Windows. Windows builds use `crawl.exe`; PowerShell environment-variable
syntax for tests is shown below. The Cargo project is in `distributed_crawler/`.

## Build and install

From a fresh checkout, enter the repository root, the directory containing this
README and `distributed_crawler/`. Install the binary using the committed lockfile:

```sh
cargo install --path distributed_crawler --bin crawl --locked
crawl --help
```

This builds an optimized binary and installs it in Cargo's binary directory, so
`crawl` can run from any directory. If the shell reports `command not found`,
add the default Cargo binary directory to the current shell's PATH:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
```

On Windows, ensure `%USERPROFILE%\.cargo\bin` is in PATH. After updating the
source, repeat the install command with `--force` to update the installed binary.

To build without installing, run from the repository root:

```sh
cargo build --release --locked --manifest-path distributed_crawler/Cargo.toml --bin crawl
./distributed_crawler/target/release/crawl --help
```

For that option, replace `crawl` in the following examples with the binary's
path. On Windows its path is `distributed_crawler/target/release/crawl.exe`.
During development, Cargo can also build and run a command directly:

```sh
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl -- node
```

## Start Redis

Run this once on the computer hosting Redis, if no container named `redis`
already exists:

```sh
docker run -d --name redis -p 6379:6379 redis:7
```

If that container already exists but is stopped, start it instead:

```sh
docker start redis
```

Wait until this check returns `PONG`; retry the check if Redis is still starting:

```sh
docker exec redis redis-cli PING
```

Only one Redis instance is needed for the cluster. Crawler nodes run directly
on their host computers, outside Docker.

## Run several nodes and jobs

The default Redis address is `redis://127.0.0.1:6379/`, database 0. The following
walkthrough runs two nodes on the Redis-hosting computer.

In terminal 1, start a node and leave it running:

```sh
crawl node
```

In terminal 2, start another node and leave it running:

```sh
crawl node
```

Repeat this command in additional terminals to run N nodes. Each node defaults
to 10 workers, with a per-node request limit shared across all its jobs. Two
nodes can have up to 20 requests in flight in total. For a slower learning run,
`crawl node --workers 1` uses one worker; accepted worker counts are 1 to 10.

In terminal 3, submit two URLs in one command:

```sh
crawl submit https://quotes.toscrape.com/ https://quotes.toscrape.com/tag/love/
```

The command prints one numeric job ID and normalized URL per input, then returns
without waiting. Each URL is both that job's starting point and its base prefix.
These jobs overlap in reachable URLs, but their work and statistics are separate:
a URL may be fetched once by each job. Node terminals log `job <id>: fetching
<url>`, showing which work each process takes.

Use the returned IDs in the following commands. `1` and `2` are examples; IDs
may differ and may contain gaps:

```sh
crawl status 1
crawl stats 1
crawl status -f 1
crawl status -f 2
crawl stats 1
crawl stats 2
```

Before completion, `stats` prints an unfinished message without partial counts.
`status -f` (also `status --follow`) prints an initial snapshot, polls Redis once
per second, prints changed snapshots, and exits after printing `done: true`.
It also exits immediately if the job is already done. Without a node, a new job
stays waiting; Ctrl-C stops the follow command without cancelling the job.

Submit a URL again to see its existing job ID, including after completion:

```sh
crawl submit https://quotes.toscrape.com/
```

Duplicate submission does not recrawl the site. To compare fresh one-node and
multi-node runs, use separate unused Redis databases for the runs, with the same
address and database on every command in each run. Public-site responses can
change; use the controlled distributed test below for exact count comparisons.

The assignment's example uses the same submission command:

```sh
crawl submit https://cs.muic.mahidol.ac.th/courses/ooc/api/
```

After all jobs finish, stop each node with Ctrl-C. Completed statistics stay in
Redis and remain readable with `crawl stats`. Do not stop a node mid-crawl for
this walkthrough: recovery of its in-flight work is not implemented.

## Redis on another computer

Use `--redis-url` on every node and CLI command to select the shared server.
For example, if its reachable address were `192.168.1.10`:

```sh
crawl --redis-url redis://192.168.1.10:6379/ node
crawl --redis-url redis://192.168.1.10:6379/ submit https://quotes.toscrape.com/
crawl --redis-url redis://192.168.1.10:6379/ status -f 1
crawl --redis-url redis://192.168.1.10:6379/ stats 1
```

Replace that example address and job ID with the actual values. `--redis-url`
can appear before or after the subcommand; a bare Redis URL is not accepted.
The `/` at the end selects database 0; `/14`, for example, selects database 14.
All nodes and CLI commands for a run must use the same server and database.

`127.0.0.1` refers to the computer running the command. A node on another
computer must use the Redis host's reachable IP. Local addresses such as
`192.168.x.x` need a shared local network or an appropriate private connection
between networks. Nodes and the CLI communicate only through Redis; no direct
node-to-node connection is needed.

## Progress and statistics

| Progress field | Meaning |
|---|---|
| `crawled` | Successfully counted files, including non-HTML files |
| `frontier` | URLs waiting to be claimed |
| `in-flight` | Claimed URLs whose processing has not finished |
| `processed` | Finished attempts, including successful files, redirects, and failures |
| `unsuccessful` | Failed HTTP responses or request/body errors |
| `done` | Both frontier and in-flight collections are empty after results are saved |

A completed job can contain unsuccessful attempts; broken links add no files or
words. The four final statistics map to `WebStats` as follows:

| CLI output | Rust field and type |
|---|---|
| `files` | `num_files: usize` |
| `extensions` | `num_exts: usize` |
| Indented extension counts | `ext_counts: HashMap<String, usize>` |
| `words` | `total_word_count: u64` |

Extension counts are printed alphabetically. An unknown job reports an error
and exits unsuccessfully. The CLI requires a subcommand; passing only a website
URL does not submit it.

## Tests

Run tests from the repository root against a dedicated Redis instance with no
other crawler nodes connected. This Docker example uses port 6380 so it can run
alongside the normal Redis container on 6379. Use an available name and port.
Persistence is disabled, and the test container is removed when stopped:

```sh
docker run -d --rm --name crawler-test-redis -p 127.0.0.1:6380:6379 redis:7 redis-server --save "" --appendonly no
docker exec crawler-test-redis redis-cli PING
CRAWLER_TEST_REDIS_URL=redis://127.0.0.1:6380/ cargo test --locked --manifest-path distributed_crawler/Cargo.toml
docker stop crawler-test-redis
```

Wait for `PONG` before the test command. In PowerShell, set the test address with
`$env:CRAWLER_TEST_REDIS_URL = "redis://127.0.0.1:6380/"`, then run the same
`cargo test` command without the environment assignment in front.

Formatting and lint checks do not need Redis:

```sh
cargo fmt --manifest-path distributed_crawler/Cargo.toml --check
cargo clippy --locked --manifest-path distributed_crawler/Cargo.toml --all-targets -- -D warnings
```

Redis tests default to `redis://127.0.0.1:6379/15`; set
`CRAWLER_TEST_REDIS_URL` to select another instance or database. Storage tests
use unique key prefixes. CLI tests submit unique test URLs and remove their
own job data afterward; the shared ID counter may advance. Tests never clear
the entire database. A failed test may leave its own test data behind.

`tests/distributed.rs` launches real host `crawl node` processes against a
controlled HTTP site, comparing one node with 1 or 10 workers, two nodes with
one worker each, and three nodes with one worker each. The expected totals are
7 files (`html: 5`, `jpg: 1`, `pdf: 1`), 3 extensions, and 10 words. Held responses
force requests to overlap across processes and verify that an empty frontier
with a page in flight stays unfinished. Several parents discover the same child;
request logs verify one fetch per URL per job and no out-of-scope requests.
Another scenario uses two nodes with two workers each and two overlapping jobs,
checking independent totals. These runs also check follow-mode exit, immediate
final stats, completed-submission reuse, and retained stats after nodes stop.

The process test refuses to start nodes if it sees existing active jobs. It
stops and reaps its child processes and removes only its own job data between
runs. HTTP fixtures use available localhost ports and stop when each test ends.
The tests require local networking access.

## Redis submission and progress

`distributed_crawler/src/store.rs` connects to Redis asynchronously. Production
keys use the `crawler:` prefix:

| Key | Redis type | Purpose |
|---|---|---|
| `crawler:next_job_id` | Integer string | Allocate candidate numeric job IDs |
| `crawler:submissions` | Hash | Map normalized base URLs to existing IDs |
| `crawler:active` | Set | Jobs available for workers to consider |
| `crawler:job:<id>:meta` | Hash | Base URL, state, and crawl counters |
| `crawler:job:<id>:seen` | Set | URLs already scheduled for this job |
| `crawler:job:<id>:frontier` | List | URLs waiting to be fetched |
| `crawler:job:<id>:inflight` | Set | Claimed URLs whose work is not finished |
| `crawler:job:<id>:extensions` | Hash | Extension counts for successful files |

Submission reserves a candidate ID, then runs
`distributed_crawler/src/lua/submit.lua`. The script returns the existing ID for a duplicate URL, including a completed job. For a
new URL, it initializes metadata, records the seed in `seen`, appends it to the
frontier, adds the job to `active`, and records the submission mapping. Redis
runs these script commands without another client's commands interleaving,
so simultaneous submissions cannot create two jobs for the same base.
Candidate IDs reserved for duplicate submissions are unused; gaps are harmless.

`distributed_crawler/src/lua/claim.lua` takes one URL from the front of the
frontier with `LPOP`, records it in the in-flight set with `SADD`, and changes the job state to
`running`. These updates execute together so two callers cannot claim the same
queue entry, and a status reader cannot see a URL removed from the queue before
it is recorded in flight. Submission and finish append through `RPUSH`, providing
shared FIFO scheduling. Unknown jobs, completed jobs, and empty queues return no work.
An empty queue never marks a job done.

`distributed_crawler/src/lua/status.lua` reads metadata and queue sizes in one consistent snapshot.
Unknown jobs return an error through the CLI.

`distributed_crawler/src/lua/finish.lua` first checks that the parent URL is still in flight. If it
is not, the result is ignored, preventing duplicate finishes from counting or
publishing links twice. Each discovered URL is inserted into `seen`; only a new
insertion is appended to the frontier. This deduplicates discoveries across
all callers for a job, including cycles and simultaneous discoveries.

Finish adds a successful file's extension and word contribution, increments
the processed count, and removes the parent from flight. HTTP/request/body
failures increment the unsuccessful count and contribute no file or words.
Redirects may publish a destination and count as processed, but contribute no
file, words, or unsuccessful count. All of these changes happen in one script.

After publishing discoveries and results, finish marks the job done only if
both the frontier and in-flight set are empty. It removes the job from `active`
but keeps its metadata, submission mapping, seen URLs, and statistics with no
expiration. Final counts are stored before the job becomes done; `num_exts`
is the number of extension hash fields.

`distributed_crawler/src/lua/stats.lua` checks completion and reads file, word, and extension counts
in one atomic snapshot. Unknown jobs return no result; unfinished jobs return
an unfinished flag without exposing partial statistics. `Store::stats` returns
the final `WebStats` only for a completed job. The CLI sorts extension names
before printing. Results remain available when nodes stop, while Redis retains
the job data; reading stats does not restart the crawl or clear its results.

For fixed website responses, changing the number of nodes changes processing
order, not the final sums: each normalized URL is scheduled and claimed once
per job, and each accepted result contributes once. Separate jobs have separate
keys, so their queues and counts do not mix. Lua scripts are embedded in the
binary at compile time; the installed CLI needs no runtime script files.

Coordination assumes the assignment's no-crash model: Redis and nodes stay
running, and there is no recovery for a worker that disappears while holding
work. Lua atomic execution prevents interleaving; it does not roll back earlier
writes if a script errors. Redis keys and script arguments are owned and
prepared by this application; repairing corrupted Redis state is not included.

## Node loop

`distributed_crawler/src/node.rs` starts a fixed number of long-lived Tokio tasks, defaulting to 10.
Each worker owns a cloned handle to the same multiplexed Redis connection and
HTTP client pool. Cloning these handles does not create separate queues.
Because a worker awaits each fetch and finish before claiming another URL,
the node has at most its worker count of HTTP requests in flight, across all
jobs. Counts outside 1 to 10 are rejected by the CLI and node API.

On each pass, a worker reads the active job IDs from Redis, sorts them, and
handles at most one queued URL per job. For each URL it calls claim, awaits the
HTTP fetch, then calls finish with either the outcome or a request/body failure.
It refreshes active jobs on every pass. When it cannot claim any work, it
sleeps for 250 milliseconds before checking again. An empty queue can mean
another node is still processing a page; only the atomic finish operation marks
a job done. FIFO order within each job gives BFS-style scheduling, without a
barrier between depths; concurrent requests may finish in a different order.

The node supervises workers with a Tokio `JoinSet`. A worker error, panic, or
unexpected exit stops the node, reports the error, and cancels the other tasks.
There is no retry or crash recovery for work held at that point.

## URL and extension rules

- Accept HTTP and HTTPS URLs. Resolve relative links against the containing page.
- Remove fragments: `page.html#intro` and `page.html#usage` are one crawl target.
- Preserve query strings and path case. Different queries or differently cased
  paths can identify different files. Query parameters are not reordered.
- Use the `url` parser's normalization of hostnames, default ports, and dot
  segments.
- Keep a link only when it has the same origin (scheme, host, and port) and its
  normalized URL starts with the submitted normalized base URL, as required by
  the assignment. We do not add a trailing slash or broaden a file base to its
  parent directory. For a directory crawl, submit a base ending in `/`: `/docs/`
  excludes `/docs-other/`, whereas the literal prefix `/docs` includes it.
- Ignore invalid links and non-web schemes such as `mailto:` and `javascript:`.
- Take the extension after the last dot in the final URL path segment and
  lowercase it. Query strings and fragments do not determine the extension.
  Percent-encoded path characters are not decoded for extension extraction.
- Use `html` for a missing or empty extension. Keep `jpg` and `jpeg` distinct;
  classify `archive.tar.gz` as `gz`.

Extension classification is separate from HTML detection. Response
`Content-Type` determines whether to parse a file as HTML.

## HTML parsing rules

- Lowercase HTML text and split it on whitespace. Count each token whose first
  character is an ASCII letter `a`–`z`. Count occurrences, not unique words.
  For example, `Hello world "Rust" 123` counts as two words: quotation marks are
  not removed. `hello-world` counts as one word.
- Exclude tags, attributes, comments, and script/style contents from word
  counting. Decode HTML entities through the HTML parser before counting.
- Treat separate text nodes as separate pieces of text, preserving element
  boundaries. `Hello<b>Rust</b>world` counts as three words. Include ordinary
  document text, including title text; no JavaScript or CSS rendering is used.
- Extract `href` references from `a`, `area`, and `link` elements, and `src`
  references from `img`, `script`, `iframe`, and `source` elements. This includes
  hyperlinks and common embedded images, stylesheets, scripts, and media sources.
  We do not extract `srcset` candidates or URLs inside CSS or JavaScript.
- Resolve links using the first parsable `base[href]` URL, or the containing page
  URL if none is valid. This affects relative-link resolution, never the job's
  original scope.
- Filter every discovery using the URL rules above and remove repeated URLs
  within a page. Redis deduplicates discoveries across workers within each job.

## HTTP fetching rules

- Reuse one async HTTP client and make one GET per claimed URL. Automatic
  redirects and retries are disabled so requests stay under the queue's control.
- A successful 2xx response contributes one file. Use `text/html` and
  `application/xhtml+xml` Content-Type values to select HTML parsing, regardless
  of URL extension. Media types are case-insensitive; parameters such as
  `charset=utf-8` are accepted. Decode HTML text using the response charset,
  defaulting to UTF-8.
- For other or missing Content-Type values, contribute the file's extension
  with zero words and no discovered links. Drop the response without intentionally
  reading its full body; GET can still receive some body bytes with the headers.
- Handle 301, 302, 303, 307, and 308 as redirects. Resolve the Location header
  against the requested URL, remove its fragment, and apply the original job's
  scope. Return an allowed target as a discovery for the shared queue to
  deduplicate and claim later. Missing, invalid, or out-of-scope targets add no
  work. Redirect aliases contribute no files or words; successful destinations
  contribute when fetched separately.
- Other non-2xx statuses, including 404 and 500, contribute no files or words.
  Request and HTML-body read errors are returned to the caller without retries.
  The node logs these failures and calls finish to release and record them.

## Troubleshooting and limits

- `crawl: command not found`: install the binary and check Cargo's bin directory
  in PATH. Alternatively, use the built executable's path from the repository root.
- Docker reports that `redis` already exists: use `docker start redis` for the
  existing stopped container instead of trying to create another with that name.
- `Could not connect to Redis`: check Docker is running, verify `PING`, and check
  the IP, published port, database, and network reachability. A timeout occurs
  before the node starts; another computer's local-network IP is not directly
  reachable from a separate network.
- A submitted job stays waiting: start a node with the same Redis URL/database.
- A completed submission produces no new fetches: reuse of its job ID is expected.
- A website returns errors such as 403 or 503: inspect the node's HTTP error log
  and the `unsuccessful` count. These responses do not contribute successful files.

The crawler follows the assignment's no-crash model. It does not recover killed
nodes, retry URLs, cancel jobs, execute JavaScript, or honor robots.txt. Different
URLs serving identical content are still different files; deduplication is by
normalized URL within each job, not by response content. No maximum crawl depth
or page-count cutoff is imposed. There is no configured HTTP request timeout;
a response that never finishes can keep a job in flight.
