use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// A real CLI process whose output is drained and which is always reaped.
pub struct Process {
    child: Child,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    readers: Vec<JoinHandle<()>>,
}

fn capture(stream: impl Read + Send + 'static) -> (Arc<Mutex<String>>, JoinHandle<()>) {
    let output = Arc::new(Mutex::new(String::new()));
    let captured = Arc::clone(&output);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stream).lines() {
            let line = line.unwrap();
            let mut output = captured.lock().unwrap();
            output.push_str(&line);
            output.push('\n');
        }
    });
    (output, reader)
}

impl Process {
    pub fn start(redis_url: &str, arguments: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_crawl"))
            .args(["--redis-url", redis_url])
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (stdout, out_reader) = capture(child.stdout.take().unwrap());
        let (stderr, err_reader) = capture(child.stderr.take().unwrap());
        Self {
            child,
            stdout,
            stderr,
            readers: vec![out_reader, err_reader],
        }
    }

    pub fn output(&self) -> String {
        self.stdout.lock().unwrap().clone()
    }

    pub fn errors(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }

    pub fn is_running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    pub async fn wait_for_output(&mut self, text: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.output().contains(text) {
                assert!(self.is_running(), "CLI exited: {}", self.errors());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("CLI did not print {text:?}: {}", self.output()));
    }

    pub async fn wait(&mut self) -> ExitStatus {
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("CLI did not exit");
        self.join_readers();
        status
    }

    fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            reader.join().unwrap();
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_readers();
    }
}
