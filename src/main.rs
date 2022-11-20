use std::io::{BufRead, Read};
use std::{process, thread};
use std::str;
use std::time::Duration;

struct ProcBuilder {
    pub program: String,
    pub args: Vec<String>,
    pub max_args: usize,
}

impl ProcBuilder {
    fn push_arg(&mut self, arg: String) -> bool {
        assert!(!self.finalized());
        self.args.push(arg);
        self.finalized()
    }

    /// Returns true when the execution is finalized (ie cannot accept more arguments)
    fn finalized(&self) -> bool {
        self.args.len() >= self.max_args
    }
}

struct ProcPool {
    max_parallelism: usize,
    next_exec: ProcBuilder,
    procs: Vec<process::Child>,
}

impl ProcPool {
    fn new(program: String, max_args: usize, max_parallelism: usize) -> ProcPool {
        ProcPool {
            max_parallelism,
            next_exec: ProcBuilder {
                program: program,
                args: vec![],
                max_args: max_args,
            },
            procs: vec![],
        }
    }

    fn push_arg(&mut self, arg: &str) {
        // we don't except the execution to be finalized without a new arg being pushed
        assert!(!self.next_exec.finalized());
        let finalized = self.next_exec.push_arg(arg.into());
        if !finalized {
            return;
        }
        self.wait_for_room();
        self.procs.push(
            process::Command::new(&self.next_exec.program)
                .args(&self.next_exec.args)
                .stdin(process::Stdio::null())
                .stdout(process::Stdio::piped())
                .spawn()
                .expect("unabled to spawn process"),
        );
        self.next_exec = ProcBuilder {
            program: self.next_exec.program.to_owned(),
            args: vec![],
            max_args: self.next_exec.max_args,
        }
    }

    fn wait_for_room(&mut self) {
        self.wait_until_len(self.max_parallelism - 1);
    }

    pub fn wait_all(&mut self) {
        self.wait_until_len(0);
    }

    fn wait_until_len(&mut self, len: usize) {
        loop {
            self.procs.retain_mut(|c| match c.try_wait() {
                Ok(None) => true,
                Ok(Some(_)) => {
                    if let Some(stdout) = c.stdout.as_mut() {
                        let mut buf = String::new();
                        let bytes_read = stdout.read_to_string(&mut buf).unwrap_or_else(|e| {
                            eprintln!("failed to read stdout: {}", e);
                            0
                        });
                        if bytes_read > 0 {
                            print!("{}", buf);
                        }
                    }
                    false
                }
                Err(e) => {
                    eprintln!("proc exited with {}", e);
                    false
                }
            });
            if self.procs.len() <= len {
                break;
            }
            // TODO: avoid this busy loop somehow
            thread::sleep(Duration::from_millis(50));
        } 
    }
}

fn main() {
    let stdin = std::io::stdin();
    let reader = stdin.lock();
    let mut split_iter = reader.split(b'\n');
    let mut pool = ProcPool::new("echo".into(), 2, 16);
    while let Some(result) = split_iter.next() {
        let buf = result.expect("failed to read argumetn buf");
        let arg = str::from_utf8(&buf).expect("argument decoding failed");
        pool.push_arg(arg);
    }
    pool.wait_all()
}
