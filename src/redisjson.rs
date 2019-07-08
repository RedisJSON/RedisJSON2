// RedisJSON Redis module.
//
// Translate between JSON and tree of Redis objects:
// User-provided JSON is converted to a tree. This tree is stored transparently in Redis.
// It can be operated on (e.g. INCR) and serialized back to JSON.
use std::mem;
use serde_json::{Value, Number};
use jsonpath_lib::{JsonPathError};

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
        Error { msg: format!("{:?}", e) }
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

        let current_data = mem::replace(&mut self.data, Value::Null);
        let new_data = jsonpath_lib::replace_with(current_data, path, &mut |_v| {
            json.clone()
        })?;
        self.data = new_data;

        Ok(())
    }

    pub fn delete_path(&mut self, path: &str) -> Result<usize, Error> {
        let current_value = mem::replace(&mut self.data, Value::Null);
        self.data = jsonpath_lib::delete(current_value, path)?;

        let res : usize = match self.data {
            Value::Null => 0,
            _ => 1
        };
        Ok(res)
    }

    pub fn to_string(&self, path: &str) -> Result<String, Error> {
        let results = self.get_doc(path)?;
        Ok(serde_json::to_string(&results)?)
    }

    pub fn str_len(&self, path: &str) -> Result<usize, Error> {
        match self.get_doc(path)?.as_str() {
            Some(s) => Ok(s.len()),
            None => Err("ERR wrong type of path value".into())
        }
    }

    pub fn get_type(&self, path: &str) -> Result<String, Error> {
        let s = RedisJSON::value_name(self.get_doc(path)?);
        Ok(s.to_string())
    }

    fn value_name(value: &Value) -> &str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    pub fn num_op<F: Fn(f64, f64) -> f64>(&mut self, path: &str, number: f64, fun: F) -> Result<String, Error> {
        let current_data = mem::replace(&mut self.data, Value::Null);
        let mut error= String::new();
        let mut result : f64 = 0.0;
        self.data = jsonpath_lib::replace_with(current_data, path, &mut |v| {
            match v {
                Value::Number(curr) => {
                    match curr.as_f64() {
                        Some(curr_value) => {
                            result = fun(curr_value, number);
                            match Number::from_f64(result) {
                                Some(new_value) => {
                                    Value::Number(new_value)
                                },
                                None => {
                                    error.push_str("ERR can not represent result as Number");
                                    v.clone()
                                }
                            }
                        },
                        None => {
                            error.push_str("ERR can not convert current value as f64");
                            v.clone()
                        }
                    }
                },
                _ => {
                    error.push_str("ERR wrong type of path value - expected a number but found ");
                    error.push_str(RedisJSON::value_name(&v));
                    v.clone()
                }
            }
        })?;
        if error == "" {
            Ok(result.to_string())
        } else {
            Err(error.into())
        }
    }

    fn get_doc<'a>(&'a self, path: &'a str) -> Result<&'a Value, Error> {
        let results = jsonpath_lib::select(&self.data, path)?;
        match results.first() {
            Some(s) => Ok(s),
            None => Ok(&Value::Null)
        }
    }
}