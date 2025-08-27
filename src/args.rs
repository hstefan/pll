pub struct AppendArgs {
    initial_args: Vec<String>,
    args: Vec<String>,
    max_args: usize,
    min_args: usize,
}

pub trait ArgBuilder {
    fn push_arg(&mut self, arg: &str) -> bool;
    fn arg_list(&self) -> Vec<String>;
    fn viable(&self) -> bool;
}

impl ArgBuilder for AppendArgs {
    fn push_arg(&mut self, arg: &str) -> bool {
        assert!(self.args.len() <= self.max_args);
        self.args.push(arg.to_owned());
        self.args.len() == self.max_args
    }

    /// Returns true when the execution is finalized (ie cannot accept more arguments)
    fn viable(&self) -> bool {
        self.args.len() >= self.min_args
    }

    fn arg_list(&self) -> Vec<String> {
        self.initial_args
            .iter()
            .cloned()
            .chain(self.args.iter().cloned())
            .collect()
    }
}

pub struct TemplateArgs {
    arg_list: Vec<TemplateArg>,
    idx: usize,
    finalized_count: usize,
}

enum TemplateArg {
    IndexedPlaceHolder(usize),
    Value(String),
}

pub enum ArgBuilderType {
    Template(TemplateArgs),
    Append(AppendArgs),
}

impl ArgBuilder for ArgBuilderType {
    fn push_arg(&mut self, arg: &str) -> bool {
        match self {
            ArgBuilderType::Append(append) => append.push_arg(arg),
            ArgBuilderType::Template(template) => template.push_arg(arg),
        }
    }

    fn arg_list(&self) -> Vec<String> {
        match self {
            ArgBuilderType::Append(append) => append.arg_list(),
            ArgBuilderType::Template(template) => template.arg_list(),
        }
    }

    fn viable(&self) -> bool {
        match self {
            ArgBuilderType::Append(append) => append.viable(),
            ArgBuilderType::Template(template) => template.viable(),
        }
    }
}

impl TemplateArgs {
    fn new(templ: Vec<String>) -> Result<TemplateArgs, String> {
        let arg_list: Vec<TemplateArg> = templ
            .iter()
            .enumerate()
            .map(|(idx, val)| match (val.find("{"), val.find("}")) {
                (None, None) => TemplateArg::Value(val.clone()),
                (Some(_), None) => TemplateArg::IndexedPlaceHolder(idx),
                (None, Some(_)) => TemplateArg::IndexedPlaceHolder(idx),
                (Some(i), Some(j)) => {
                    let actual_idx = if j < i || j == i + 1 {
                        idx
                    } else if let Ok(templ_idx) = val[i + 1..j].parse::<usize>() {
                        templ_idx
                    } else {
                        idx
                    };
                    TemplateArg::IndexedPlaceHolder(actual_idx)
                }
            })
            .collect();
        let finalized_count = arg_list
            .iter()
            .filter(|x| matches!(x, TemplateArg::Value(_)))
            .count();
        Ok(TemplateArgs {
            arg_list,
            idx: 0,
            finalized_count,
        })
    }
}

impl ArgBuilder for TemplateArgs {
    fn push_arg(&mut self, arg: &str) -> bool {
        assert!(!self.viable());
        for i in 0..self.arg_list.len() {
            if let TemplateArg::IndexedPlaceHolder(templ_idx) = self.arg_list[i]
                && self.idx == templ_idx {
                    self.arg_list[i] = TemplateArg::Value(arg.to_owned());
                    self.finalized_count += 1;
                }
        }
        self.idx += 1;
        self.viable()
    }

    fn arg_list(&self) -> Vec<String> {
        self.arg_list
            .iter()
            .filter_map(|r| match r {
                TemplateArg::Value(v) => Some(v.clone()),
                _ => None,
            })
            .collect()
    }

    fn viable(&self) -> bool {
        self.finalized_count == self.arg_list.len()
    }
}
pub trait ArgBuilderMaker<T: ArgBuilder> {
    fn make(&self) -> T;
}

pub struct DynArgBuilderMaker {
    pub is_template: bool,
    pub initial_args: Vec<String>,
    pub max_args: usize,
    pub min_args: usize,
}

impl ArgBuilderMaker<ArgBuilderType> for DynArgBuilderMaker {
    fn make(&self) -> ArgBuilderType {
        if self.is_template {
            ArgBuilderType::Template(TemplateArgs::new(self.initial_args.clone()).unwrap())
        } else {
            ArgBuilderType::Append(AppendArgs {
                initial_args: self.initial_args.clone(),
                args: vec![],
                max_args: self.max_args,
                min_args: self.min_args,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::{AppendArgs, ArgBuilder, TemplateArgs};

    #[test]
    fn append_args_works() {
        let mut builder = AppendArgs {
            initial_args: vec!["initial".into()],
            args: vec![],
            max_args: 2,
            min_args: 1,
        };
        builder.push_arg("foo");
        assert!(builder.push_arg("bar"));
        assert_eq!(builder.arg_list(), ["initial", "foo", "bar"]);
    }

    #[test]
    fn append_args_min_works() {
        let mut builder = AppendArgs {
            initial_args: vec!["initial".into()],
            args: vec![],
            max_args: 2,
            min_args: 1,
        };
        assert!(!builder.push_arg("foo"));
        assert!(builder.viable());
        assert_eq!(builder.arg_list(), ["initial", "foo"]);
    }

    #[test]
    fn template_args_works() {
        let mut builder =
            TemplateArgs::new(vec!["initial".into(), "{0}".into(), "{1}".into()]).unwrap();
        builder.push_arg("foo");
        assert!(builder.push_arg("bar"));
        assert_eq!(builder.arg_list(), ["initial", "foo", "bar"]);
    }
}
