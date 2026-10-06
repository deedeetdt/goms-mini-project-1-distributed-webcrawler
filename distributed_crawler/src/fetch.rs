use reqwest::header::{CONTENT_TYPE, LOCATION};
use url::Url;

use crate::page::{PageAnalysis, analyze_html};
use crate::url_rules::{file_extension, resolve_link};

#[derive(Debug)]
pub enum FetchOutcome {
    File {
        extension: String,
        analysis: PageAnalysis,
    },
    Redirect {
        target: Option<Url>,
    },
    Unsuccessful {
        status: u16,
    },
}

/// Reuse this client for the requests made by a node's workers.
#[derive(Clone)]
pub struct Fetcher {
    client: reqwest::Client,
}

impl Fetcher {
    pub fn new() -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self { client })
    }

    /// Fetch one normalized, in-scope URL claimed from the job's queue.
    /// Request/body errors are returned so the worker can report and finish them.
    pub async fn fetch(&self, base: &Url, url: &Url) -> Result<FetchOutcome, reqwest::Error> {
        let response = self.client.get(url.clone()).send().await?;
        let status = response.status();

        if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            let target = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|location| resolve_link(base, url, location));
            // The shared queue will deduplicate and claim the destination later.
            return Ok(FetchOutcome::Redirect { target });
        }

        if !status.is_success() {
            return Ok(FetchOutcome::Unsuccessful {
                status: status.as_u16(),
            });
        }

        let is_html = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|content_type| content_type.split(';').next())
            .is_some_and(|media_type| {
                let media_type = media_type.trim();
                media_type.eq_ignore_ascii_case("text/html")
                    || media_type.eq_ignore_ascii_case("application/xhtml+xml")
            });

        let analysis = if is_html {
            let source = response.text().await?;
            analyze_html(base, url, &source)
        } else {
            // Dropping the response avoids intentionally reading the binary body.
            PageAnalysis::default()
        };

        Ok(FetchOutcome::File {
            extension: file_extension(url),
            analysis,
        })
    }
}
