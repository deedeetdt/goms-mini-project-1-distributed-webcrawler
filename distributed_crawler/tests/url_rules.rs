use distributed_crawler::url_rules::{file_extension, is_in_scope, normalize_url, resolve_link};
use url::Url;

#[test]
fn fragment_variants_have_the_same_identity() {
    let intro = normalize_url("https://example.org/docs/page.html#intro").unwrap();
    let usage = normalize_url("https://example.org/docs/page.html#usage").unwrap();

    assert_eq!(intro, usage);
    assert_eq!(intro.as_str(), "https://example.org/docs/page.html");
}

#[test]
fn query_variants_keep_distinct_identities() {
    let first = normalize_url("https://example.org/docs/page?id=1#intro").unwrap();
    let second = normalize_url("https://example.org/docs/page?id=2#intro").unwrap();

    assert_ne!(first, second);
    assert_eq!(first.query(), Some("id=1"));
    assert_eq!(second.query(), Some("id=2"));
}

#[test]
fn path_case_is_preserved() {
    let upper = normalize_url("https://example.org/docs/About.html").unwrap();
    let lower = normalize_url("https://example.org/docs/about.html").unwrap();

    assert_ne!(upper, lower);
    assert_eq!(upper.path(), "/docs/About.html");
}

#[test]
fn hostname_and_default_port_are_normalized() {
    let explicit = normalize_url("https://EXAMPLE.org:443/docs/").unwrap();
    let ordinary = normalize_url("https://example.org/docs/").unwrap();

    assert_eq!(explicit, ordinary);
}

#[test]
fn invalid_or_non_web_submission_urls_are_rejected() {
    for input in [
        "not a URL",
        "mailto:someone@example.org",
        "javascript:alert(1)",
        "file:///tmp/page.html",
    ] {
        assert!(
            normalize_url(input).is_err(),
            "unexpectedly accepted {input}"
        );
    }

    assert!(normalize_url("http://127.0.0.1:8080/site/").is_ok());
}

#[test]
fn relative_links_are_resolved_against_the_containing_page() {
    let base = Url::parse("https://example.org/docs/").unwrap();
    let page = Url::parse("https://example.org/docs/chapter/index.html").unwrap();

    let target = resolve_link(&base, &page, "../about.html#team").unwrap();

    assert_eq!(target.as_str(), "https://example.org/docs/about.html");
}

#[test]
fn fragment_only_links_resolve_to_the_current_page() {
    let base = Url::parse("https://example.org/docs/").unwrap();
    let page = Url::parse("https://example.org/docs/page.html").unwrap();

    assert_eq!(resolve_link(&base, &page, "#section"), Some(page.clone()));
}

#[test]
fn scope_requires_the_original_origin_and_url_prefix() {
    let base = Url::parse("https://example.org/docs/").unwrap();
    let allowed = Url::parse("https://example.org/docs/page.html").unwrap();
    assert!(is_in_scope(&base, &allowed));

    for input in [
        "https://elsewhere.org/docs/page.html",
        "https://example.org.evil.org/docs/page.html",
        "http://example.org/docs/page.html",
        "https://example.org:8443/docs/page.html",
        "https://example.org/other/page.html",
        "https://example.org/docs-other/page.html",
    ] {
        assert!(
            !is_in_scope(&base, &Url::parse(input).unwrap()),
            "accepted {input}"
        );
    }
}

#[test]
fn links_leaving_scope_or_using_non_web_schemes_are_ignored() {
    let base = Url::parse("https://example.org/docs/").unwrap();
    let page = Url::parse("https://example.org/docs/index.html").unwrap();

    for link in [
        "../outside.html",
        "//elsewhere.org/docs/index.html",
        "mailto:someone@example.org",
        "javascript:alert(1)",
        "http://[",
    ] {
        assert!(
            resolve_link(&base, &page, link).is_none(),
            "accepted {link}"
        );
    }

    assert!(resolve_link(&base, &page, "next.html").is_some());
}

#[test]
fn a_base_without_a_trailing_slash_keeps_the_literal_prefix_rule() {
    let base = Url::parse("https://example.org/docs").unwrap();
    let candidate = Url::parse("https://example.org/docs-other/page.html").unwrap();

    assert!(is_in_scope(&base, &candidate));
    assert_eq!(base.path(), "/docs");
}

#[test]
fn a_file_base_is_not_broadened_to_its_parent_directory() {
    let base = Url::parse("https://example.org/docs/index.html").unwrap();

    assert!(resolve_link(&base, &base, "other.html").is_none());
    assert_eq!(resolve_link(&base, &base, "#section"), Some(base.clone()));
}

#[test]
fn extensions_come_from_the_last_path_segment_not_the_query() {
    for (input, expected) in [
        ("https://example.org/PIC.JPEG", "jpeg"),
        ("https://example.org/pic.jpg", "jpg"),
        ("https://example.org/pic.jpeg", "jpeg"),
        ("https://example.org/api/", "html"),
        ("https://example.org/docs/intro", "html"),
        ("https://example.org/a.pdf?download=1", "pdf"),
        ("https://example.org/intro?filename=a.pdf", "html"),
        ("https://example.org/docs.v1/intro", "html"),
        ("https://example.org/archive.tar.gz", "gz"),
        ("https://example.org/file.", "html"),
    ] {
        let url = Url::parse(input).unwrap();
        assert_eq!(
            file_extension(&url),
            expected,
            "wrong extension for {input}"
        );
    }
}
