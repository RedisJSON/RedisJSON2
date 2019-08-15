// RedisJSON Redis module.
//
// Translate between JSON and tree of Redis objects:
// User-provided JSON is converted to a tree. This tree is stored transparently in Redis.
// It can be operated on (e.g. INCR) and serialized back to JSON.
use jsonpath_lib::{JsonPathError, SelectorMut};
use redismodule::raw;
use serde_json::{Value, Map};
use std::os::raw::{c_int, c_void};
use bson::decode_document;
use std::io::Cursor;
use std::mem;

#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Error { msg: e }
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Error { msg: e.to_string() }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error { msg: e.to_string() }
    }
}

impl From<JsonPathError> for Error {
    fn from(e: JsonPathError) -> Self {
        Error {
            msg: format!("{:?}", e),
        }
    }
}

impl From<Error> for redismodule::RedisError {
    fn from(e: Error) -> Self {
        redismodule::RedisError::String(e.msg)
    }
}

#[derive(Debug, PartialEq)]
pub enum Format {
    JSON,
    BSON,
}

impl Format {
    pub fn from_str(s: &str) -> Result<Format, Error> {
        match s {
            "JSON" => Ok(Format::JSON),
            "BSON" => Ok(Format::BSON),
            _ => return Err("ERR wrong format".into()),
        }
    }
}

#[derive(Debug)]
pub struct RedisJSON {
    data: Value,
}

impl RedisJSON {
    pub fn parse_str(data: &str, format: Format) -> Result<Value, Error> {
        let value: Value = match format {
            Format::JSON => serde_json::from_str(data)?,
            Format::BSON => match decode_document(&mut Cursor::new(data.as_bytes())) {
                Ok(d) => {
                    let mut iter = d.iter();
                    if d.len() >= 1 {
                        match iter.next() {
                            Some((_, b)) => b.clone().into(),
                            None => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                Err(e) => return Err(e.to_string().into()),
            },
        };
        Ok(value)
    }

    pub fn from_str(data: &str, format: Format) -> Result<Self, Error> {
        let value = RedisJSON::parse_str(data, format)?;
        Ok(Self { data: value })
    }

    pub fn set_value(&mut self, data: &str, path: &str, format: Format) -> Result<(), Error> {
        let json: Value = RedisJSON::parse_str(data, format)?;
        if path == "$" {
            self.data = json;
            Ok(())
        } else {
            let mut replaced = false;
            let current_data = self.data.take();
            self.data = jsonpath_lib::replace_with(current_data, path, &mut |_v| {
                replaced = true;
                json.clone()
            })?;
            if replaced {
                Ok(())
            } else {
                Err(format!("ERR missing path {}", path).into())
            }
        }
    }

    pub fn delete_path(&mut self, path: &str) -> Result<usize, Error> {
        let current_data = self.data.take();

        let mut deleted = 0;
        self.data = jsonpath_lib::replace_with(current_data, path, &mut |v| {
            if !v.is_null() {
                deleted = deleted + 1; // might delete more than a single value
            }
            Value::Null
        })?;
        Ok(deleted)
    }

    pub fn to_string(&self, path: &str, format: Format) -> Result<String, Error> {
        let results = self.get_doc(path)?;
        let res = match format {
            Format::JSON => serde_json::to_string(&results)?,
            Format::BSON => return Err("Soon to come...".into()) //results.into() as Bson,
        };
        Ok(res)
    }

    pub fn to_json(&self, paths: &mut Vec<String>) -> Result<String, Error> {
        let mut selector = jsonpath_lib::selector(&self.data);
        let mut result = paths.drain(..).fold(String::from("{"), |mut acc, path| {
            let value = match selector(&path) {
                Ok(s) => match s.first() {
                    Some(v) => v,
                    None => &Value::Null,
                },
                Err(_) => &Value::Null,
            };
            acc.push('\"');
            acc.push_str(&path);
            acc.push_str("\":");
            acc.push_str(value.to_string().as_str());
            acc.push(',');
            acc
        });
        if result.ends_with(",") {
            result.pop();
        }
        result.push('}');
        Ok(result.into())
    }

    pub fn str_len(&self, path: &str) -> Result<usize, Error> {
        match self.get_doc(path)?.as_str() {
            Some(s) => Ok(s.len()),
            None => Err("ERR wrong type of path value".into()),
        }
    }

    pub fn arr_len(&self, path: &str) -> Result<usize, Error> {
        match self.get_doc(path)?.as_array() {
            Some(s) => Ok(s.len()),
            None => Err("ERR wrong type of path value".into()),
        }
    }

    pub fn obj_len(&self, path: &str) -> Result<usize, Error> {
        match self.get_doc(path)?.as_object() {
            Some(s) => Ok(s.len()),
            None => Err("ERR wrong type of path value".into()),
        }
    }

    pub fn obj_keys<'a>(&'a self, path: &'a str) -> Result<Vec<&'a String>, Error> {
        match self.get_doc(path)?.as_object() {
            Some(o) => Ok(o.keys().collect()),
            None => Err("ERR wrong type of path value".into()),
        }
    }

    pub fn arr_index(
        &self,
        path: &str,
        scalar: &str,
        start: usize,
        end: usize,
    ) -> Result<i64, Error> {
        if let Value::Array(arr) = self.get_doc(path)? {
            match serde_json::from_str(scalar)? {
                Value::Array(_) | Value::Object(_) => Ok(-1),
                v => {
                    let mut start = start.max(0);
                    let end = end.min(arr.len() - 1);
                    start = end.min(start);

                    let slice = &arr[start..=end];
                    match slice.iter().position(|r| r == &v) {
                        Some(i) => Ok((start + i) as i64),
                        None => Ok(-1),
                    }
                }
            }
        } else {
            Ok(-1)
        }
    }

    pub fn get_type(&self, path: &str) -> Result<String, Error> {
        let s = RedisJSON::value_name(self.get_doc(path)?);
        Ok(s.to_string())
    }

    pub fn value_name(value: &Value) -> &str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    pub fn value_op<F>(&mut self, path: &str, mut fun: F) -> Result<Value, Error>
    where
        F: FnMut(&Value) -> Result<Value, Error>,
    {
        let current_data = self.data.take();

        let mut errors = vec![];
        let mut result = Value::Null; // TODO handle case where path not found

        let mut collect_fun = |value: Value| {
            fun(&value)
                .map(|new_value| {
                    result = new_value.clone();
                    new_value
                })
                .map_err(|e| {
                    errors.push(e);
                })
                .unwrap_or(value)
        };

        self.data = if path == "$" {
            // root needs special handling
            collect_fun(current_data)
        } else {
            SelectorMut::new()
                .str_path(path)
                .and_then(|selector| {
                    Ok(selector
                        .value(current_data.clone())
                        .replace_with(&mut |v| collect_fun(v.to_owned()))?
                        .take()
                        .unwrap_or(Value::Null))
                })
                .map_err(|e| {
                    errors.push(e.into());
                })
                .unwrap_or(current_data)
        };

        match errors.len() {
            0 => Ok(result),
            1 => Err(errors.remove(0)),
            _ => Err(errors.into_iter().map(|e| e.msg).collect::<String>().into()),
        }
    }

    pub fn get_memory<'a>(&'a self, path: &'a str) -> Result<usize, Error> {
        let res = match self.get_doc(path)? {
            Value::Null => 0,
            Value::Bool(_v) => mem::size_of::<bool>(),
            Value::Number(v ) => {
                if v.is_f64() {
                    mem::size_of::<f64>()
                } else if v.is_i64() {
                    mem::size_of::<i64>()
                } else if v.is_u64() {
                    mem::size_of::<u64>()
                } else {
                    return Err("unknown Number type".into())
                }
            }
            Value::String(_v) => mem::size_of::<String>(),
            Value::Array(_v) => mem::size_of::<Vec<Value>>(),
            Value::Object(_v) => mem::size_of::<Map<String, Value>>(),
        };
        Ok(res.into())
    }

    fn get_doc<'a>(&'a self, path: &'a str) -> Result<&'a Value, Error> {
        let results = jsonpath_lib::select(&self.data, path)?;
        match results.first() {
            Some(s) => Ok(s),
            None => Err("ERR path does not exist".into()),
        }
    }
}

#[allow(non_snake_case, unused)]
pub unsafe extern "C" fn json_rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
    if encver < 2 {
        panic!("Can't load old RedisJSON RDB"); // TODO add support for backward
    }
    let json = RedisJSON::from_str(&raw::load_string(rdb), Format::JSON).unwrap();
    Box::into_raw(Box::new(json)) as *mut c_void
}

#[allow(non_snake_case, unused)]
#[no_mangle]
pub unsafe extern "C" fn json_free(value: *mut c_void) {
    Box::from_raw(value as *mut RedisJSON);
}

#[allow(non_snake_case, unused)]
#[no_mangle]
pub unsafe extern "C" fn json_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let json = &*(value as *mut RedisJSON);
    raw::save_string(rdb, &json.data.to_string());
}
