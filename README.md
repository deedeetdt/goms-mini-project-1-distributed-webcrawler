# Mini Project 1: Distributed Webcrawler

A distributed web crawler project in Rust.

## Current progress

The project defines the crawl statistics, handles URLs, extracts links and
word counts from HTML, and fetches individual URLs asynchronously. The `crawl`
binary currently prints empty statistics; Redis coordination and the node/CLI
integration are still being implemented.

## Run and test

From the repository root:

```sh
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl
cargo test --manifest-path distributed_crawler/Cargo.toml
```

The HTTP tests start temporary servers on localhost using available ports and
stop them when each test finishes. They require local networking permission;
they do not require Docker or Redis.

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
