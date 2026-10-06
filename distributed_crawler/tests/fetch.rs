mod support;

use std::time::Duration;

use distributed_crawler::fetch::{FetchOutcome, Fetcher};
use tokio::time::timeout;

use support::{Response, Site};

fn file(outcome: FetchOutcome) -> (String, distributed_crawler::page::PageAnalysis) {
    let FetchOutcome::File {
        extension,
        analysis,
    } = outcome
    else {
        panic!("Expected a successful file, got {outcome:?}");
    };
    (extension, analysis)
}

#[tokio::test]
async fn fetches_extensionless_html_with_one_get() {
    let site = Site::start(vec![(
        "/site/intro?lang=en",
        Response::new(200, "<p>Ant ant 123</p><a href='child.html#intro'></a>")
            .header("Content-Type", "text/html; charset=utf-8"),
    )])
    .await;
    let fetcher = Fetcher::new().unwrap();

    let (extension, analysis) = file(
        fetcher
            .fetch(&site.base, &site.url("intro?lang=en"))
            .await
            .unwrap(),
    );

    assert_eq!(extension, "html");
    assert_eq!(analysis.word_count, 2);
    assert_eq!(analysis.links, vec![site.url("child.html")]);
    assert_eq!(
        site.requests(),
        vec![("GET".to_owned(), "/site/intro?lang=en".to_owned())]
    );
}

#[tokio::test]
async fn content_type_selects_html_even_with_a_jpg_suffix() {
    let site = Site::start(vec![(
        "/site/page.JPG",
        Response::new(200, "<p>Hello Rust</p>").header("Content-Type", "text/html"),
    )])
    .await;
    let fetcher = Fetcher::new().unwrap();

    let (extension, analysis) = file(
        fetcher
            .fetch(&site.base, &site.url("page.JPG"))
            .await
            .unwrap(),
    );

    assert_eq!(extension, "jpg");
    assert_eq!(analysis.word_count, 2);
}

#[tokio::test]
async fn recognizes_case_insensitive_html_and_xhtml_media_types() {
    let site = Site::start(vec![
        (
            "/site/mixed",
            Response::new(200, "<p>Hello</p>").header("Content-Type", "Text/HTML; charset=UTF-8"),
        ),
        (
            "/site/xhtml",
            Response::new(200, "<p>Rust</p>").header("Content-Type", "application/xhtml+xml"),
        ),
    ])
    .await;
    let fetcher = Fetcher::new().unwrap();

    for path in ["mixed", "xhtml"] {
        let (_, analysis) = file(fetcher.fetch(&site.base, &site.url(path)).await.unwrap());
        assert_eq!(analysis.word_count, 1);
    }
}

#[tokio::test]
async fn counts_binary_from_headers_without_waiting_for_its_body() {
    let mut response = Response::new(200, "").header("Content-Type", "image/jpeg");
    response.declared_length = Some(1_000_000);
    response.hold_open = true;
    let site = Site::start(vec![("/site/pic.JPG", response)]).await;
    let fetcher = Fetcher::new().unwrap();

    let outcome = timeout(
        Duration::from_secs(2),
        fetcher.fetch(&site.base, &site.url("pic.JPG")),
    )
    .await
    .expect("A binary response must not wait for the body")
    .unwrap();
    let (extension, analysis) = file(outcome);

    assert_eq!(extension, "jpg");
    assert_eq!(analysis.word_count, 0);
    assert!(analysis.links.is_empty());
    assert_eq!(site.requests().len(), 1);
}

#[tokio::test]
async fn does_not_guess_html_from_an_html_suffix_or_missing_content_type() {
    let site = Site::start(vec![
        (
            "/site/plain.html",
            Response::new(200, "<p>Not HTML</p>").header("Content-Type", "text/plain"),
        ),
        ("/site/no-type.html", Response::new(200, "<p>No header</p>")),
    ])
    .await;
    let fetcher = Fetcher::new().unwrap();

    for path in ["plain.html", "no-type.html"] {
        let (extension, analysis) = file(fetcher.fetch(&site.base, &site.url(path)).await.unwrap());
        assert_eq!(extension, "html");
        assert_eq!(analysis.word_count, 0);
        assert!(analysis.links.is_empty());
    }
}

#[tokio::test]
async fn accepts_other_success_statuses_as_files() {
    let site = Site::start(vec![("/site/empty", Response::new(204, ""))]).await;
    let fetcher = Fetcher::new().unwrap();
    let (_, analysis) = file(fetcher.fetch(&site.base, &site.url("empty")).await.unwrap());
    assert_eq!(analysis.word_count, 0);
}

#[tokio::test]
async fn unsuccessful_statuses_do_not_contribute_files_or_html() {
    let site = Site::start(vec![
        (
            "/site/missing",
            Response::new(404, "<p>Error words</p>").header("Content-Type", "text/html"),
        ),
        (
            "/site/error",
            Response::new(500, "<p>Error words</p>").header("Content-Type", "text/html"),
        ),
        ("/site/unchanged", Response::new(304, "")),
        (
            "/site/choices",
            Response::new(300, "").header("Location", "target.html"),
        ),
    ])
    .await;
    let fetcher = Fetcher::new().unwrap();

    for (path, expected) in [
        ("missing", 404),
        ("error", 500),
        ("unchanged", 304),
        ("choices", 300),
    ] {
        let outcome = fetcher.fetch(&site.base, &site.url(path)).await.unwrap();
        assert!(matches!(outcome, FetchOutcome::Unsuccessful { status } if status == expected));
    }
    assert_eq!(site.requests().len(), 4);
}

#[tokio::test]
async fn returns_redirect_targets_without_fetching_them() {
    let paths = [
        "/site/r301",
        "/site/r302",
        "/site/r303",
        "/site/r307",
        "/site/r308",
    ];
    let routes = paths
        .into_iter()
        .zip([301, 302, 303, 307, 308])
        .map(|(path, status)| {
            (
                path,
                Response::new(status, "<p>Alias words</p>")
                    .header("Location", "target.html#section")
                    .header("Content-Type", "text/html"),
            )
        })
        .collect();
    let site = Site::start(routes).await;
    let fetcher = Fetcher::new().unwrap();

    for path in paths {
        let outcome = fetcher.fetch(&site.base, &site.url(path)).await.unwrap();
        let FetchOutcome::Redirect { target } = outcome else {
            panic!("Expected a redirect");
        };
        assert_eq!(target, Some(site.url("target.html")));
    }
    let requests = site.requests();
    assert_eq!(requests.len(), 5);
    assert!(
        requests
            .iter()
            .all(|(method, path)| method == "GET" && path.starts_with("/site/r"))
    );
}

#[tokio::test]
async fn rejects_out_of_scope_redirects_without_fetching_them() {
    let site = Site::start(vec![
        (
            "/site/outside",
            Response::new(302, "").header("Location", "/outside.html"),
        ),
        (
            "/site/external",
            Response::new(302, "").header("Location", "https://example.com/other/"),
        ),
    ])
    .await;
    let fetcher = Fetcher::new().unwrap();

    for path in ["outside", "external"] {
        let outcome = fetcher.fetch(&site.base, &site.url(path)).await.unwrap();
        assert!(matches!(outcome, FetchOutcome::Redirect { target: None }));
    }
    assert_eq!(site.requests().len(), 2);
}

#[tokio::test]
async fn handles_missing_invalid_and_non_web_redirect_locations() {
    let site = Site::start(vec![
        ("/site/missing", Response::new(302, "")),
        (
            "/site/invalid",
            Response::new(302, "").header("Location", "http://["),
        ),
        (
            "/site/mail",
            Response::new(302, "").header("Location", "mailto:test@example.com"),
        ),
    ])
    .await;
    let fetcher = Fetcher::new().unwrap();

    for path in ["missing", "invalid", "mail"] {
        let outcome = fetcher.fetch(&site.base, &site.url(path)).await.unwrap();
        assert!(matches!(outcome, FetchOutcome::Redirect { target: None }));
    }
}

#[tokio::test]
async fn returns_body_read_errors_without_retrying_the_url() {
    let mut response = Response::new(200, "<p>Incomplete").header("Content-Type", "text/html");
    response.declared_length = Some(1_000);
    let site = Site::start(vec![("/site/truncated", response)]).await;
    let fetcher = Fetcher::new().unwrap();

    let result = fetcher.fetch(&site.base, &site.url("truncated")).await;

    assert!(result.is_err());
    assert_eq!(site.requests().len(), 1);
}
