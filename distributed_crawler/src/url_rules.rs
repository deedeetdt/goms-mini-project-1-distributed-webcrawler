use url::Url;

/// Parse a submitted web URL and remove its fragment from the crawl identity.
pub fn normalize_url(input: &str) -> Result<Url, String> {
    let mut url = Url::parse(input).map_err(|error| format!("Invalid URL: {error}"))?;

    if !matches!(url.scheme(), "http" | "https") {
        return Err("Only HTTP and HTTPS URLs are supported".to_owned());
    }

    url.set_fragment(None);
    Ok(url)
}

/// Apply the assignment's literal URL-prefix rule to normalized URLs.
pub fn is_in_scope(base: &Url, candidate: &Url) -> bool {
    matches!(candidate.scheme(), "http" | "https")
        && candidate.origin() == base.origin()
        && candidate.as_str().starts_with(base.as_str())
}

/// Resolve a page's link and keep it only if it belongs to the original job.
pub fn resolve_link(base: &Url, page: &Url, link: &str) -> Option<Url> {
    let mut candidate = page.join(link).ok()?;
    candidate.set_fragment(None);

    if is_in_scope(base, &candidate) {
        Some(candidate)
    } else {
        None
    }
}

/// Classify the last path suffix; a missing or empty suffix counts as HTML.
pub fn file_extension(url: &Url) -> String {
    let filename = url.path().rsplit('/').next().unwrap_or("");

    match filename.rsplit_once('.') {
        Some((_, extension)) if !extension.is_empty() => extension.to_lowercase(),
        _ => "html".to_owned(),
    }
}
