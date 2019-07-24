// RedisJSON Redis module.
//
// Translate between JSON and tree of Redis objects:
// User-provided JSON is converted to a tree. This tree is stored transparently in Redis.
// It can be operated on (e.g. INCR) and serialized back to JSON.
use jsonpath_lib::JsonPathError;
use serde_json::Value;
use std::cmp;

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

#[derive(Debug)]
pub struct RedisJSON {
    data: Value,
}

impl RedisJSON {
    pub fn from_str(data: &str) -> Result<Self, Error> {
        // Parse the string of data into serde_json::Value.
        let v: Value = serde_json::from_str(data)?;

        Ok(Self { data: v })
    }

    pub fn set_value(&mut self, data: &str, path: &str) -> Result<(), Error> {
        // Parse the string of data into serde_json::Value.
        let json: Value = serde_json::from_str(data)?;

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

    pub fn to_string(&self, path: &str) -> Result<String, Error> {
        let results = self.get_doc(path)?;
        Ok(serde_json::to_string(&results)?)
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
                    let mut start = cmp::max(start, 0);
                    let end = cmp::min(end, arr.len());
                    start = cmp::min(end, start);

                    let slice = &arr[start..end];
                    match slice.iter().position(|r| r == &v) {
                        Some(i) => Ok(i as i64),
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

    pub fn value_op<F: FnMut(&Value) -> Result<Value, Error>>(
        &mut self,
        path: &str,
        mut fun: F,
    ) -> Result<String, Error> {
        let current_data = self.data.take();

        let mut errors = vec![];
        let mut result = String::new(); // TODO handle case where path not found

        self.data = jsonpath_lib::replace_with(current_data, path, &mut |v| match fun(v) {
            Ok(new_value) => {
                result = new_value.to_string();
                new_value
            }
            Err(e) => {
                errors.push(e);
                v.clone()
            }
        })?;
        let err_len = errors.len();
        if err_len == 0 {
            Ok(result)
        } else if err_len == 1 {
            Err(errors.remove(0))
        } else {
            let errors_string = errors.iter().map(|e| e.msg.to_string()).collect::<String>();
            Err(errors_string.into())
        }
    }

    fn get_doc<'a>(&'a self, path: &'a str) -> Result<&'a Value, Error> {
        let results = jsonpath_lib::select(&self.data, path)?;
        match results.first() {
            Some(s) => Ok(s),
            None => Err("ERR path does not exist".into()),
        }
    }
}
