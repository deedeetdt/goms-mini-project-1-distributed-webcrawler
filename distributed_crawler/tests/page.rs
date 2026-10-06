use distributed_crawler::page::analyze_html;
use url::Url;

fn analyze(source: &str) -> distributed_crawler::page::PageAnalysis {
    let base = Url::parse("https://example.org/docs/").unwrap();
    let page = Url::parse("https://example.org/docs/index.html").unwrap();
    analyze_html(&base, &page, source)
}

#[test]
fn whitespace_words_must_begin_with_an_ascii_letter_after_lowercasing() {
    let result = analyze(r#"<p>Ant ant 123 4ever zebra "Rust" Éclair hello-world</p>"#);

    assert_eq!(result.word_count, 4);
}

#[test]
fn tags_attributes_comments_scripts_and_styles_do_not_add_words() {
    let result = analyze(
        r#"
        <p title="hidden attribute words">Ant ant</p>
        <!-- hidden comment words -->
        <script>const secret = "hidden script words";</script>
        <style>body { content: "hidden style words"; }</style>
        "#,
    );

    assert_eq!(result.word_count, 2);
}

#[test]
fn adjacent_text_nodes_are_kept_separate() {
    let result = analyze("<p>Hello<b>Rust</b>world</p>");

    assert_eq!(result.word_count, 3);
}

#[test]
fn html_entities_are_decoded_before_counting_words() {
    let result = analyze("<p>Ant&nbsp;ant &amp; &#90;ebra</p>");

    assert_eq!(result.word_count, 3);
}

#[test]
fn hyperlinks_and_common_embedded_resources_are_extracted() {
    let result = analyze(
        r#"
        <a href="next.html">Read</a>
        <area href="map.html">
        <link rel="stylesheet" href="style.css">
        <img src="pic.JPG" alt="attribute words">
        <script src="app.js"></script>
        <iframe src="frame.html"></iframe>
        <video><source src="clip.mp4"></video>
        "#,
    );
    let mut links: Vec<_> = result.links.iter().map(Url::as_str).collect();
    links.sort_unstable();

    assert_eq!(
        links,
        vec![
            "https://example.org/docs/app.js",
            "https://example.org/docs/clip.mp4",
            "https://example.org/docs/frame.html",
            "https://example.org/docs/map.html",
            "https://example.org/docs/next.html",
            "https://example.org/docs/pic.JPG",
            "https://example.org/docs/style.css",
        ]
    );
    assert_eq!(result.word_count, 1);
}

#[test]
fn repeated_links_and_fragment_variants_are_returned_once() {
    let result = analyze(
        r#"
        <a href="next.html#intro"></a>
        <a href="next.html#usage"></a>
        <iframe src="next.html"></iframe>
        "#,
    );

    assert_eq!(result.links.len(), 1);
    assert_eq!(
        result.links[0].as_str(),
        "https://example.org/docs/next.html"
    );
}

#[test]
fn invalid_and_out_of_scope_references_are_filtered() {
    let result = analyze(
        r#"
        <a href="../outside.html"></a>
        <a href="javascript:alert(1)"></a>
        <a href="mailto:someone@example.org"></a>
        <a href="http://["></a>
        <img src="https://elsewhere.org/pic.jpg">
        <a href="inside.html"></a>
        "#,
    );

    assert_eq!(result.links.len(), 1);
    assert_eq!(
        result.links[0].as_str(),
        "https://example.org/docs/inside.html"
    );
}

#[test]
fn the_first_valid_html_base_resolves_relative_links() {
    let result = analyze(
        r#"
        <base href="/docs/assets/">
        <base href="/docs/ignored/">
        <a href="next.html"></a>
        "#,
    );

    assert_eq!(result.links.len(), 1);
    assert_eq!(
        result.links[0].as_str(),
        "https://example.org/docs/assets/next.html"
    );
}

#[test]
fn an_invalid_html_base_does_not_hide_later_valid_bases() {
    let result = analyze(
        r#"
        <base href="http://[">
        <base href="/docs/assets/">
        <a href="next.html"></a>
        "#,
    );

    assert_eq!(result.links.len(), 1);
    assert_eq!(
        result.links[0].as_str(),
        "https://example.org/docs/assets/next.html"
    );
}

#[test]
fn an_html_base_cannot_change_the_original_job_scope() {
    let result = analyze(
        r#"
        <base href="https://elsewhere.org/">
        <a href="outside.html"></a>
        <img src="outside.jpg">
        <a href="https://example.org/docs/inside.html"></a>
        "#,
    );

    assert_eq!(result.links.len(), 1);
    assert_eq!(
        result.links[0].as_str(),
        "https://example.org/docs/inside.html"
    );
}

#[test]
fn imperfect_html_still_yields_text_and_links() {
    let result = analyze(r#"<p>Hello <b>world<p>Again<a href="next.html">Read"#);

    assert_eq!(result.word_count, 4);
    assert_eq!(result.links.len(), 1);
}
