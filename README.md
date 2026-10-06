# Mini Project 1: Distributed Webcrawler

A distributed web crawler project in Rust.

## Current progress

The CLI can submit jobs to Redis and show their status. It reuses the existing
job ID when the same normalized URL is submitted again. URL handling, HTML
analysis, and individual HTTP fetching are also implemented.

The storage layer can atomically claim a waiting URL and record it in flight.
This operation is tested but is not yet called by a running worker.

Workers and the crawling loop are not implemented yet: submitted jobs remain
waiting. `node`, `status -f`, and `stats` will be added in later steps.

## Run and test

From the repository root:

```sh
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl -- --help
```

Start Redis in Docker if you do not already have a container named `redis`:

```sh
docker run -d --name redis -p 6379:6379 redis:7
```

If that container already exists but is stopped, use `docker start redis`.
Then submit a URL and inspect the job ID printed by the command:

```sh
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl -- submit https://cs.muic.mahidol.ac.th/courses/ooc/api/
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl -- status 1
```

Replace `1` with the returned job ID. Expect zero crawled files, one URL in the
frontier, zero in flight, and `done: false`. Submitting the URL again returns the
same ID and leaves only one waiting URL. Each URL in a multi-URL submission
gets its own job; all inputs are validated before any of that batch's jobs are
created. A Redis error during submission can still interrupt the batch.

Use `--redis-url` to connect to another address:

```sh
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl -- --redis-url redis://127.0.0.1:6379/ status 1
```

For the full test suite, keep Redis running:

```sh
cargo test --manifest-path distributed_crawler/Cargo.toml
cargo fmt --manifest-path distributed_crawler/Cargo.toml --check
cargo clippy --manifest-path distributed_crawler/Cargo.toml --all-targets -- -D warnings
```

Redis tests default to `redis://127.0.0.1:6379/15`; set
`CRAWLER_TEST_REDIS_URL` to use another test instance or database. Storage tests
use unique key prefixes. CLI tests submit unique test URLs and remove their
own job data afterward; the shared ID counter may advance. Tests never clear
the entire database. A failed test may leave its own test data behind.

HTTP tests start temporary servers on localhost using available ports and stop
them when each test finishes. The tests require local networking permission.

## Redis submission and progress

`src/store.rs` connects to Redis asynchronously. Production keys use the
`crawler:` prefix:

| Key | Redis type | Purpose |
|---|---|---|
| `crawler:next_job_id` | Integer string | Allocate candidate numeric job IDs |
| `crawler:submissions` | Hash | Map normalized base URLs to existing IDs |
| `crawler:active` | Set | Jobs available for workers to consider |
| `crawler:job:<id>:meta` | Hash | Base URL, state, and crawl counters |
| `crawler:job:<id>:seen` | Set | URLs already scheduled for this job |
| `crawler:job:<id>:frontier` | List | URLs waiting to be fetched |
| `crawler:job:<id>:inflight` | Set | Claimed URLs whose work is not finished |

Submission reserves a candidate ID, then runs `src/lua/submit.lua`. The script
returns the existing ID for a duplicate URL, including a completed job. For a
new URL, it initializes metadata, records the seed in `seen`, appends it to the
frontier, adds the job to `active`, and records the submission mapping. Redis
runs these script commands without another client's commands interleaving,
so simultaneous submissions cannot create two jobs for the same base.
Candidate IDs reserved for duplicate submissions are unused; gaps are harmless.

`src/lua/claim.lua` takes one URL from the front of the frontier with `LPOP`,
records it in the in-flight set with `SADD`, and changes the job state to
`running`. These updates execute together so two callers cannot claim the same
queue entry, and a status reader cannot see a URL removed from the queue before
it is recorded in flight. Submission appends through `RPUSH`; the finish
operation will use that same append rule for discoveries, providing shared FIFO
scheduling. Unknown jobs, completed jobs, and empty queues return no work.
An empty queue never marks a job done.

`src/lua/status.lua` reads metadata and queue sizes in one consistent snapshot.
Unknown jobs return an error through the CLI. Finishing and automatic completion
detection are still to be implemented. Each claimed URL will stay in flight
until its results and discoveries are published; completion will require both
an empty frontier and an empty in-flight set.

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
  within a page. Cluster-wide deduplication will be handled by Redis.

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
  Request and HTML-body read errors are returned to the caller without retries;
  node integration will report and finish those failed attempts through Redis.
