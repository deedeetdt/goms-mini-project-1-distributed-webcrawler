# Mini Project 1: Distributed Webcrawler

A distributed web crawler project in Rust.

## Current progress

The project defines the crawl statistics and URL handling rules. The `crawl`
binary currently prints empty statistics; HTML parsing, HTTP crawling, and Redis
coordination are still being implemented.

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
