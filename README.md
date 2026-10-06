# Mini Project 1: Distributed Webcrawler

A distributed web crawler project in Rust.

## Current progress

The project defines the crawl statistics, handles URLs, and extracts links and
word counts from HTML. The `crawl` binary currently prints empty statistics;
HTTP crawling and Redis coordination are still being implemented.

## Run and test

From the repository root:

```sh
cargo run --manifest-path distributed_crawler/Cargo.toml --bin crawl
cargo test --manifest-path distributed_crawler/Cargo.toml
```

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

Extension classification is separate from HTML detection. When HTTP fetching is
added, response `Content-Type` will determine whether to parse a file as HTML.

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
