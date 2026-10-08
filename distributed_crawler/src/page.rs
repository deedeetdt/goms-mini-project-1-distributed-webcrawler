use std::collections::HashSet;

use scraper::{Html, Node, Selector};
use url::Url;

use crate::url_rules::resolve_link;

#[derive(Debug, Default)]
pub struct PageAnalysis {
    pub links: Vec<Url>,
    pub word_count: u64,
}

/// Extract owned results so the HTML document never needs to cross an await.
pub fn analyze_html(base: &Url, page_url: &Url, source: &str) -> PageAnalysis {
    let document = Html::parse_document(source);
    let link_base = html_link_base(&document, page_url);
    let link_selector = Selector::parse(
        "a[href], area[href], link[href], img[src], script[src], iframe[src], source[src]",
    )
    .expect("The fixed link selector is valid");

    let mut links = Vec::new();
    let mut seen = HashSet::new();

    for element in document.select(&link_selector) {
        let attribute = match element.value().name() {
            "a" | "area" | "link" => "href",
            _ => "src",
        };
        let Some(reference) = element.value().attr(attribute) else {
            continue;
        };
        let Some(link) = resolve_link(base, &link_base, reference) else {
            continue;
        };

        if seen.insert(link.clone()) {
            links.push(link);
        }
    }

    PageAnalysis {
        links,
        word_count: document_word_count(&document),
    }
}

fn html_link_base(document: &Html, page_url: &Url) -> Url {
    let selector = Selector::parse("base[href]").expect("The fixed base selector is valid");

    for element in document.select(&selector) {
        let Some(reference) = element.value().attr("href") else {
            continue;
        };
        if let Ok(base) = page_url.join(reference) {
            return base;
        }
    }

    page_url.clone()
}

fn document_word_count(document: &Html) -> u64 {
    let mut count = 0;

    for node in document.root_element().descendants() {
        let Node::Text(text) = node.value() else {
            continue;
        };
        let inside_script_or_style = node.ancestors().any(|ancestor| match ancestor.value() {
            Node::Element(element) => matches!(element.name(), "script" | "style"),
            _ => false,
        });
        if inside_script_or_style {
            continue;
        }

        // Count each text node separately to preserve element boundaries.
        for word in text.to_lowercase().split_whitespace() {
            if word
                .chars()
                .next()
                .is_some_and(|first| first.is_ascii_lowercase())
            {
                count += 1;
            }
        }
    }

    count
}
