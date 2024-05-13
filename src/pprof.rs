use anyhow::Error;
use prost::Message;
use std::io::Write;
use std::{collections::HashMap, hash::Hash, io};

use crate::config::Config;
use crate::proto_gen::perftools::profiles as protobuf;
use crate::stack_trace::{Frame, StackTrace};

fn unset<D: Default>() -> D {
    Default::default()
}

type StringIndex = i64;
#[derive(Hash, PartialEq, Eq, Clone)]
struct FunctionData {
    name: StringIndex,
    filename: StringIndex,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct LocationData {
    function_id: u64,
    line: StringIndex,
}

pub struct PProf {
    config: Config,
    string_index: HashMap<String, i64>,
    function_id: HashMap<FunctionData, u64>,
    location_id: HashMap<LocationData, u64>,
    sample_index: HashMap<u64, HashMap<Vec<u64>, usize>>,
    profile: protobuf::Profile,
}

impl PProf {
    pub fn new(config: &Config) -> Self {
        let mut me = Self {
            config: config.clone(),
            string_index: Default::default(),
            function_id: Default::default(),
            location_id: Default::default(),
            sample_index: Default::default(),
            profile: protobuf::Profile {
                sample_type: vec![],
                sample: vec![],
                mapping: unset(),
                location: vec![],
                function: vec![],
                string_table: vec![],
                drop_frames: unset(),
                keep_frames: unset(),
                time_nanos: unset(), // nice to have, but we don't have this data currently
                duration_nanos: unset(), // nice to have, but we don't have this data currently
                period_type: None,
                period: 1_000_000_000 / config.sampling_rate as i64,
                comment: unset(),
                default_sample_type: unset(),
            },
        };
        me.get_string_index("");

        let r#type = me.get_string_index("count");
        me.profile
            .sample_type
            .push(protobuf::ValueType { r#type, unit: 0 });

        let r#type = me.get_string_index("cpu");
        let unit = me.get_string_index("nanoseconds");
        me.profile.period_type = Some(protobuf::ValueType { r#type, unit });
        me
    }

    fn get_string_index(&mut self, str: &str) -> StringIndex {
        if let Some(idx) = self.string_index.get(str) {
            return *idx;
        }
        let i = self.profile.string_table.len() as i64;
        self.string_index.insert(str.to_string(), i);
        self.profile.string_table.push(str.to_string());
        i
    }

    fn get_function_id(&mut self, key: FunctionData) -> u64 {
        if let Some(id) = self.function_id.get(&key) {
            return *id;
        }
        let id = self.profile.function.len() as u64 + 1;
        self.profile.function.push(protobuf::Function {
            id,
            name: key.name,
            system_name: unset(),
            filename: key.filename,
            start_line: unset(), // denotes the line of the function, which we don't have currently
        });
        self.function_id.insert(key, id);
        id
    }

    fn get_location_id(&mut self, key: LocationData) -> u64 {
        if let Some(id) = self.location_id.get(&key) {
            return *id;
        }
        let id: u64 = self.profile.location.len() as u64 + 1;
        self.profile.location.push(protobuf::Location {
            id,
            mapping_id: unset(),
            address: unset(),
            line: vec![protobuf::Line {
                function_id: key.function_id,
                line: if self.config.show_line_numbers {
                    key.line
                } else {
                    unset()
                },
                column: unset(),
            }],
            is_folded: unset(),
        });
        self.location_id.insert(key, id);
        id
    }

    fn add_frame(&mut self, frame: &Frame) -> u64 {
        let name = self.get_string_index(&frame.name);
        let filename = self.get_string_index(&frame.filename);
        let function_id = self.get_function_id(FunctionData { name, filename });
        
        self.get_location_id(LocationData {
            function_id,
            line: frame.line as i64,
        })
    }

    fn make_label(&mut self, key: &str, value: &str) -> protobuf::Label {
        let thread_id_label = self.get_string_index(key);
        let thread_name_index = self.get_string_index(value);
        protobuf::Label {
            key: thread_id_label,
            str: thread_name_index,
            num: unset(),
            num_unit: 0,
        }
    }

    fn make_label_num(&mut self, key: &str, num: i64) -> protobuf::Label {
        let thread_id_label = self.get_string_index(key);
        protobuf::Label {
            key: thread_id_label,
            str: unset(),
            num,
            num_unit: 0,
        }
    }

    fn get_sample_index(&mut self, frames: &[u64], stack: &StackTrace) -> usize {
        // thread ids are unique system-wide
        let innermap = self.sample_index.entry(stack.thread_id).or_insert(Default::default());
        if let Some(i) = innermap.get(frames) {
            return *i;
        }

        let i: usize = self.profile.sample.len();
        innermap.insert(frames.to_vec(), i);
        

        let mut label = vec![];
        if let Some(name) = &stack.thread_name {
            label.push(self.make_label("thread_name", name));
        }
        label.push(self.make_label_num("thread_id", stack.thread_id as i64));
        label.push(self.make_label_num("pid", stack.pid as i64));

        self.profile.sample.push(protobuf::Sample {
            location_id: frames.to_vec(),
            value: vec![0],
            label,
        });

        i
    }

    pub fn record(&mut self, stack: &StackTrace) -> Result<(), io::Error> {
        let frames = stack
            .frames
            .iter()
            .map(|frame| self.add_frame(frame))
            .collect::<Vec<_>>();

        let sample_index = self.get_sample_index(&frames, stack);

        self.profile.sample[sample_index].value[0] += 1;
        Ok(())
    }

    pub fn write_all(&self, w: &mut dyn Write) -> Result<(), Error> {
        w.write_all(&self.profile.encode_to_vec())?;
        Ok(())
    }
}
