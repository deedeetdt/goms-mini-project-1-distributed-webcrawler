use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use url::Url;

pub struct Response {
    pub status: u16,
    pub headers: Vec<(&'static str, &'static str)>,
    pub body: &'static str,
    pub declared_length: Option<usize>,
    pub hold_open: bool,
}

impl Response {
    pub fn new(status: u16, body: &'static str) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
            declared_length: None,
            hold_open: false,
        }
    }

    pub fn header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((name, value));
        self
    }
}

pub struct Site {
    pub base: Url,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    task: JoinHandle<()>,
}

impl Site {
    pub async fn start(routes: Vec<(&str, Response)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = Url::parse(&format!("http://{}/site/", listener.local_addr().unwrap())).unwrap();
        let routes: Arc<HashMap<String, Response>> = Arc::new(
            routes
                .into_iter()
                .map(|(path, response)| (path.to_owned(), response))
                .collect(),
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let task = tokio::spawn(async move {
            // Aborting the server also drops and aborts any open connections.
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (mut stream, _) = connection.unwrap();
                        let routes = Arc::clone(&routes);
                        let request_log = Arc::clone(&request_log);
                        connections.spawn(async move {
                            let mut request = Vec::new();
                            let mut buffer = [0; 1024];
                            while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                                let size = stream.read(&mut buffer).await.unwrap();
                                if size == 0 { return; }
                                request.extend_from_slice(&buffer[..size]);
                            }
                            let request = String::from_utf8(request).unwrap();
                            let mut parts = request.lines().next().unwrap().split_whitespace();
                            let method = parts.next().unwrap();
                            let path = parts.next().unwrap();
                            request_log.lock().unwrap().push((method.to_owned(), path.to_owned()));

                            let fallback = Response::new(404, "Missing");
                            let response = routes.get(path).unwrap_or(&fallback);
                            let length = response.declared_length.unwrap_or(response.body.len());
                            let mut reply = format!(
                                "HTTP/1.1 {} Fixture\r\nContent-Length: {length}\r\nConnection: close\r\n",
                                response.status,
                            );
                            for (name, value) in &response.headers {
                                reply.push_str(&format!("{name}: {value}\r\n"));
                            }
                            reply.push_str("\r\n");
                            reply.push_str(response.body);
                            if stream.write_all(reply.as_bytes()).await.is_err() { return; }

                            if response.hold_open {
                                // Simulate a binary body that never finishes arriving.
                                // The fetcher must return from headers alone.
                                let _ = stream.read(&mut buffer).await;
                            }
                        });
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
        });

        Self {
            base,
            requests,
            task,
        }
    }

    pub fn url(&self, reference: &str) -> Url {
        self.base.join(reference).unwrap()
    }

    pub fn requests(&self) -> Vec<(String, String)> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for Site {
    fn drop(&mut self) {
        self.task.abort();
    }
}
