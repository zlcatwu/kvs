#![deny(missing_docs)]
//! KvStore lib code

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write, BufWriter};
use std::path::PathBuf;
use failure::Fail;

/// KvStore result type
pub type Result<T> = std::result::Result<T, KvStoreError>;

/// KvStore error type
#[derive(Debug, Fail)]
pub enum KvStoreError {

    /// Can not create KvStore with the given path
    #[fail(display = "file can not be opened: {}", msg)]
    FileOpenError {
        /// Message from the error
        msg: String
    },

    /// The given key not found in the KvStore
    #[fail(display = "key not found: {}", key)]
    KeyNotFound {
        /// The given key
        key: String
    },

    /// Command in file is incorrect
    #[fail(display = "command convert error: {}", msg)]
    CommandConvertError {
        /// Message from the error
        msg: String
    },

    /// Unexpected error
    #[fail(display = "unknown error: {}", msg)]
    UnknownError {
        /// Message from the error
        msg: String
    },
}

impl From<std::io::Error> for KvStoreError {
    fn from(error: std::io::Error) -> Self {
        KvStoreError::FileOpenError {
            msg: error.to_string(),
        }
    }
}

impl From<ron::Error> for KvStoreError {
    fn from(error: ron::Error) -> Self {
        KvStoreError::CommandConvertError {
            msg: error.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

/// Used to create a representation of a key-value store
pub struct KvStore {
    file_handle: File,
    map: HashMap<String, u64>,
    is_build: bool,
    count_of_set: u64,
}

impl KvStore {
    /// Open the KvStore with the given dir path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path: PathBuf = path.into();
        path.push(".kvs_store");
        let file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(KvStore {
            file_handle,
            map: HashMap::new(),
            is_build: false,
            count_of_set: 0,
        })
    }

    /// Store value with the key
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let mut cmd = ron::to_string(&cmd)?;
        cmd.push('\n');
        let offset = self.file_handle.seek(SeekFrom::End(0))?;
        self.file_handle.write_all(cmd.as_bytes())?;
        self.map.insert(key, offset);
        self.count_of_set += 1;
        if self.count_of_set > 100 {
            self.compaction()?;
            self.count_of_set = 0;
        }

        Ok(())
    }

    /// Get the value from the given key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.build_map()?;
        if let Some(&offset) = self.map.get(&key) {
            let value = self.fetch_value(offset)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Remove the given key
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.build_map()?;
        if !self.map.contains_key(&key) {
            return Err(KvStoreError::KeyNotFound { key });
        }
        let cmd = Command::Remove { key: key.clone() };
        let mut cmd = ron::to_string(&cmd)?;
        cmd.push('\n');
        self.file_handle.seek(SeekFrom::End(0))?;
        self.file_handle.write_all(cmd.as_bytes())?;
        self.map.remove(&key);

        Ok(())
    }

    fn fetch_value(&mut self, offset: u64) -> Result<String> {
        self.file_handle.seek(SeekFrom::Start(offset))?;
        let mut cmd = String::new();
        let mut reader = BufReader::new(&self.file_handle);
        reader.read_line(&mut cmd)?;
        if let Command::Set { key: _, value } = ron::from_str(&cmd)? {
            Ok(value)
        } else {
            Err(KvStoreError::UnknownError {
                msg: "Command info not matched".to_owned(),
            })
        }
    }

    fn build_map(&mut self) -> Result<()> {
        if self.is_build {
            return Ok(())
        }
        let mut cur_offset = self.file_handle.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&self.file_handle);

        loop {
            let mut cmd = String::new();
            let offset = reader.read_line(&mut cmd)? as u64;
            if offset == 0 {
                break;
            }
            let cmd: Command = ron::from_str(&cmd)?;
            match cmd {
                Command::Set { key, value: _ } => {
                    self.map.insert(key, cur_offset);
                }
                Command::Remove { key } => {
                    self.map.remove(&key);
                }
            }
            cur_offset += offset;
        }

        self.is_build = true;
        Ok(())
    }

    fn compaction(&mut self) -> Result<()> {
        self.build_map()?;
        let mut compacted_data: Vec<u8> = Vec::new();
        {
            let mut compacted_writer = BufWriter::new(&mut compacted_data);
            let entries: Vec<(String, u64)> = self.map.iter().map(|(key, &offset)| (key.clone(), offset)).collect();
            for (key, offset) in entries {
                let value = self.fetch_value(offset)?;
                let cmd = Command::Set { key, value };
                let mut cmd = ron::to_string(&cmd)?;
                cmd.push('\n');
                compacted_writer.write_all(cmd.as_bytes())?;
            }
        }
        self.file_handle.set_len(0)?;
        self.file_handle.seek(SeekFrom::Start(0))?;
        self.file_handle.write_all(&compacted_data)?;
        self.is_build = false;

        Ok(())
    }
}
