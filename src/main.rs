use clap::Parser;
use std::io::{BufRead, Read};

use std::str;
use std::time::Duration;
use std::{process, thread};

mod args;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    /// A string with all the characters that will be used to split arguments
    delim: Option<String>,

    #[arg(short = '0', long = "null")]
    /// Use null character ('\0') as a separator
    ///
    /// If a delimiter string is provide alongside this flag, the null character will be added to that list.
    null_sep: bool,

    #[arg(short = 'p', long, default_value_t = 16)]
    max_parallelism: usize,

    #[arg(short = 'n', long = "max-args", default_value_t = 1)]
    /// Controls the max number of arguments that can be used to spawn a program
    ///
    /// This is ignored when using a template.
    max_args_count: usize,

    #[arg(long = "min-args", default_value_t = 1)]
    /// Determines the min number of arguments required to spawn a program
    ///
    /// Only relevant in case there aren't enough arguments to fill up to max count. This is ignored when using
    /// a template.
    min_args_count: usize,

    #[arg(long, default_value_t = false)]
    /// When enabled the output of each execution will only be written to stdout after the process exits
    ///
    /// Useful when it's undesireable to stream the ouput of several programs running in parallel.
    pipe_stdout: bool,

    #[arg(short = 'l', long = "template")]
    /// When enabled the program strings will be processed as a template
    ///
    /// Example: "ssh {0}@{2} {1}" will read three arguments and replace the appropriate indices before spawning the process
    template: bool,

    program: Vec<String>,
}

struct ProcPool<T: args::ArgBuilder, U: args::ArgBuilderMaker<T>> {
    program: String,
    max_parallelism: usize,
    proc_builder: T,
    proc_builder_fn: U,
    procs: Vec<process::Child>,
    pipe_stdout: bool,
}

impl<T: args::ArgBuilder, U: args::ArgBuilderMaker<T>> ProcPool<T, U> {
    fn new(
        program: String,
        proc_builder_fn: U,
        max_parallelism: usize,
        pipe_stdout: bool,
    ) -> ProcPool<T, U> {
        ProcPool {
            program,
            max_parallelism,
            proc_builder: proc_builder_fn.make(),
            proc_builder_fn,
            procs: vec![],
            pipe_stdout,
        }
    }

    fn push_arg(&mut self, arg: &str) {
        let finalized = self.proc_builder.push_arg(arg);
        if !finalized {
            return;
        }
        self.wait_for_room();
        self.spawn();
    }

    fn wait_for_room(&mut self) {
        self.wait_until_len(self.max_parallelism - 1);
    }

    pub fn wait_all(&mut self) {
        if self.proc_builder.viable() {
            self.wait_for_room();
            self.spawn();
        }
        self.wait_until_len(0);
    }

    fn spawn(&mut self) {
        let stdout_cfg = if self.pipe_stdout {
            process::Stdio::piped()
        } else {
            process::Stdio::inherit()
        };
        self.procs.push(
            process::Command::new(&self.program)
                .args(self.proc_builder.arg_list())
                .stdin(process::Stdio::null())
                .stdout(stdout_cfg)
                .spawn()
                .expect("unabled to spawn process"),
        );
        self.proc_builder = self.proc_builder_fn.make();
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

trait ManySplit<B> {
    fn split_any(self, delims: &[u8]) -> SplitMany<B>;
}

struct SplitMany<B> {
    buf: B,
    delims: Vec<u8>,
    next: Vec<u8>,
}

impl<B: BufRead> Iterator for SplitMany<B> {
    type Item = std::io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<std::io::Result<Vec<u8>>> {
        loop {
            if let Some(pos) = self.next.iter().position(|x| self.delims.contains(x)) {
                // TODO: don't re-scan characters that we already checked
                return Some(Ok(self.next.drain(0..pos + 1).collect()));
            }
            let amt = match self.buf.fill_buf() {
                Ok(bytes) => {
                    self.next.extend_from_slice(bytes);
                    bytes.len()
                }
                Err(e) => return Some(Err(e)),
            };
            self.buf.consume(amt);
            if amt == 0 {
                return if self.next.is_empty() {
                    None
                } else {
                    Some(Ok(self.next.drain(0..).collect()))
                };
            }
        }
    }
}

impl<B: BufRead> ManySplit<B> for B {
    fn split_any(self, delims: &[u8]) -> SplitMany<B> {
        SplitMany {
            buf: self,
            delims: delims.to_vec(),
            next: vec![],
        }
    }
}

fn clean_arg<'a>(delims: &[u8], arg: &'a [u8]) -> Option<&'a str> {
    if let Some(start) = arg.iter().position(|x| !delims.contains(x)) {
        if let Some(end) = arg
            .iter()
            .skip(start)
            .rev()
            .position(|x| !delims.contains(x))
        {
            Some(str::from_utf8(&arg[start..(arg.len() - end)]).expect("argument decoding failed"))
        } else {
            None
        }
    } else {
        None
    }
}

fn main() {
    let args = Args::parse();

    if args.min_args_count > args.max_args_count {
        eprintln!("min arg count cannot be larger than max count");
        return;
    }

    let delims = match (args.delim, args.null_sep) {
        (None, true) => vec![b'\0'],
        (None, false) => vec![b'\n', b'\t', b' '],
        (Some(v), null_sep) => {
            let mut d = v.as_bytes().to_vec();
            if null_sep {
                d.push(b'\0');
            }
            d
        }
    };
    let program = args.program.first().map(AsRef::as_ref).unwrap_or("echo");
    let initial_args = args.program.iter().skip(1).map(|v| v.to_owned()).collect();

    let stdin = std::io::stdin();
    let reader = stdin.lock();
    let split_iter = reader.split_any(&delims);

    let proc_builder = args::DynArgBuilderMaker {
        initial_args,
        is_template: args.template,
        max_args: args.max_args_count,
        min_args: args.min_args_count,
    };

    let mut pool = ProcPool::new(
        program.into(),
        proc_builder,
        args.max_parallelism,
        args.pipe_stdout,
    );
    for result in split_iter {
        let buf = result.expect("failed to read argument buf");
        if let Some(arg) = clean_arg(&delims, &buf) {
            pool.push_arg(arg);
        }
    }
    pool.wait_all()
}
