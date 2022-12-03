use clap::Parser;
use std::io::{BufRead, Read};
use std::str;
use std::time::Duration;
use std::{process, thread};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    delim: Option<String>,

    #[arg(short = 'p', long, default_value_t = 16)]
    max_parallelism: usize,

    #[arg(short = 'n', long = "args", default_value_t = 1)]
    num_args: usize,

    #[arg(long, default_value_t = false)]
    /// Causes the output of each execution will only be output after the process exits.
    ///
    /// Useful when it's undesireable to stream the ouput of several programs running in parallel.
    pipe_stdout: bool,

    program: Vec<String>,
}

struct ProcBuilder {
    pub program: String,
    pub initial_args: Vec<String>,
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
    pipe_stdout: bool,
}

impl ProcPool {
    fn new(
        program: String,
        initial_args: Vec<String>,
        max_args: usize,
        max_parallelism: usize,
        pipe_stdout: bool,
    ) -> ProcPool {
        ProcPool {
            max_parallelism,
            next_exec: ProcBuilder {
                program,
                initial_args,
                args: vec![],
                max_args,
            },
            procs: vec![],
            pipe_stdout,
        }
    }

    fn push_arg(&mut self, arg: &str) {
        // we don't expect the execution to be finalized without a new arg being pushed
        assert!(!self.next_exec.finalized());
        let finalized = self.next_exec.push_arg(arg.into());
        if !finalized {
            return;
        }
        self.wait_for_room();
        let stdout_cfg = if self.pipe_stdout {
            process::Stdio::piped()
        } else {
            process::Stdio::inherit()
        };
        self.procs.push(
            process::Command::new(&self.next_exec.program)
                .args(&self.next_exec.initial_args)
                .args(&self.next_exec.args)
                .stdin(process::Stdio::null())
                .stdout(stdout_cfg)
                .spawn()
                .expect("unabled to spawn process"),
        );
        self.next_exec = ProcBuilder {
            program: self.next_exec.program.to_owned(),
            args: vec![],
            max_args: self.next_exec.max_args,
            initial_args: self.next_exec.initial_args.to_owned(),
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
                        // this path is only triggered when stdout is piped instead of inherited
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
    let args = Args::parse();

    let delim = args.delim.map_or_else(|| b'\n', |v| v.as_bytes()[0]);
    let program = args.program.get(0).map(AsRef::as_ref).unwrap_or("echo");
    let initial_args = args.program.iter().skip(1).map(|v| v.to_owned()).collect();

    let stdin = std::io::stdin();
    let reader = stdin.lock();
    let split_iter = reader.split(delim);

    let mut pool = ProcPool::new(
        program.into(),
        initial_args,
        args.num_args,
        args.max_parallelism,
        args.pipe_stdout,
    );
    for result in split_iter {
        let buf = result.expect("failed to read argument buf");
        let arg = str::from_utf8(&buf).expect("argument decoding failed");
        pool.push_arg(arg);
    }
    pool.wait_all()
}
