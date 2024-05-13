use anyhow::Error;
use prost::Message;
use py_spy::{stack_trace, Config, Frame};
use std::io::Write;
use std::{borrow::Borrow, collections::HashMap, hash::Hash, io};

use crate::proto_gen::perftools::profiles as pprof;

fn unset<D: Default>() -> D {
    Default::default()
}

struct Interner<K, V> {
    map: HashMap<K, i64>,
    table: Vec<V>,
}

impl<K: Hash + Eq, V> Interner<K, V> {
    fn new() -> Self {
        Self {
            map: Default::default(),
            table: vec![],
        }
    }

    // or_val argument: next id
    fn get_index<'q, Q>(&mut self, key: &'q Q, or_val: impl Fn(u64) -> V) -> i64
    where
        K: Borrow<Q> + From<&'q Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.get(key).copied().unwrap_or_else(|| {
            let i = self.table.len() as i64;
            self.table.push(or_val(self.table.len() as u64 + 1));
            self.map.insert(key.into(), i);
            i
        })
    }

    fn get(&self, index: i64) -> &V {
        &self.table[index as usize]
    }

    fn get_mut(&mut self, index: i64) -> &mut V {
        &mut self.table[index as usize]
    }
}

struct PProf {
    config: Config,
    string_table: Interner<String, String>,
    function: Interner<String, pprof::Function>,
    location: Interner<String, pprof::Location>,
    sample: Interner<Vec<u64>, pprof::Sample>,
}

impl PProf {
    pub fn new(config: &Config) -> Self {
        let mut string_table = Interner::new();
        string_table.get_index("", |_| String::from(""));
        Self {
            config: config.clone(),
            string_table,
            function: Interner::new(),
            location: Interner::new(),
            sample: Interner::new(),
        }
    }

    fn get_string_index(&mut self, str: &str) -> i64 {
        self.string_table.get_index(str, |_| str.into())
    }

    fn get_location_id(&mut self, frame: &Frame) -> u64 {
        let name = self.get_string_index(&frame.name);
        let filename = self.get_string_index(&frame.filename);
        let function_absolute_name = format!("{}:{}", frame.filename, frame.name);
        let function_index: i64 = self.function.get_index(&function_absolute_name, |id| {
            pprof::Function {
                id,
                name,
                system_name: name,
                filename,
                start_line: unset(), // denotes the line of the function, which we don't have currently
            }
        });
        let location_absolute_name = format!("{}/{}", function_absolute_name, frame.line);
        let location_index = self.location.get_index(&location_absolute_name, |id| {
            let function = self.function.get(function_index);
            let line = pprof::Line {
                function_id: function.id,
                line: frame.line as i64,
                column: unset(),
            };
            pprof::Location {
                id,
                mapping_id: unset(),
                address: unset(),
                line: vec![line],
                is_folded: false,
            }
        });
        let location = self.location.get(location_index);
        location.id
    }

    pub fn record(&mut self, stack: &stack_trace::StackTrace) -> Result<(), io::Error> {
        let frames = stack
            .frames
            .iter()
            .map(|frame| self.get_location_id(frame))
            .collect::<Vec<_>>();

        let sample_index = self
            .sample
            .get_index(frames.as_slice(), |_| pprof::Sample {
                location_id: frames.clone(),
                value: vec![0],
                label: vec![],
            });
        self.sample.get_mut(sample_index).value[0] += 1;
        Ok(())
    }

    fn foo(&mut self) -> pprof::Profile {
        pprof::Profile {
            sample_type: vec![pprof::ValueType {
                r#type: self.get_string_index("count"),
                unit: self.get_string_index("times"),
            }],
            sample: self.sample.table.clone(),
            mapping: unset(),
            location: self.location.table.clone(),
            function: self.function.table.clone(),
            string_table: self.string_table.table.clone(),
            drop_frames: unset(),
            keep_frames: unset(),
            time_nanos: unset(), // nice to have, but we don't have this data currently
            duration_nanos: unset(), // nice to have, but we don't have this data currently
            period_type: Some(pprof::ValueType {
                r#type: self.get_string_index("cpu"),
                unit: self.get_string_index("nanoseconds"),
            }),
            period: 1_000_000_000 / self.config.sampling_rate as i64,
            comment: vec![],
            default_sample_type: unset(),
        }
    }

    pub fn write(&mut self, w: &mut dyn Write) -> Result<(), Error> {
        let profile = self.foo();
        w.write_all(&profile.encode_to_vec())?;
        Ok(())
    }
}
