#[macro_use]
extern crate redismodule;

use redismodule::native_types::RedisType;
use redismodule::{Context, NextArg, RedisError, RedisResult, REDIS_OK};
use serde_json::{Number, Value};
use std::{cmp, i64, usize};

mod redisjson;

use crate::redisjson::{Error, Format, RedisJSON};

static JSON_TYPE_ENCODING_VERSION: i32 = 2;
static JSON_TYPE_NAME: &str = "ReJSON-RL";

static REDIS_JSON_TYPE: RedisType = RedisType::new(
    JSON_TYPE_NAME,
    JSON_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,

        rdb_load: Some(redisjson::json_rdb_load),
        rdb_save: Some(redisjson::json_rdb_save),
        aof_rewrite: None, // TODO add support
        free: Some(redisjson::json_free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,
    },
);

#[derive(Debug, PartialEq)]
pub enum SetOptions {
    NotExists,
    AlreadyExists,
    None,
}

///
/// backward compatibility convector for RedisJSON 1.x clients
///
fn backward_path(mut path: String) -> String {
    if !path.starts_with("$") {
        if path == "." {
            path.replace_range(..1, "$");
        } else if path.starts_with(".") {
            path.insert(0, '$');
        } else {
            path.insert_str(0, "$.");
        }
    }
    return path;
}

///
/// JSON.DEL <key> [path]
///
fn json_del(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);

    let key = ctx.open_key_writable(&key);
    let deleted = match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => {
            if path == "$" {
                key.delete()?;
                1
            } else {
                doc.delete_path(&path)?
            }
        }
        None => 0,
    };
    Ok(deleted.into())
}

///
/// JSON.SET <key> <path> <json> [NX | XX]
///
fn json_set(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);
    let value = args.next_string()?;

    let mut format = Format::JSON;
    let mut set_option = SetOptions::None;
    loop {
        if let Some(s) = args.next() {
            match s.as_str() {
                "NX" => set_option = SetOptions::NotExists,
                "XX" => set_option = SetOptions::AlreadyExists,
                "FORMAT" => {
                    format = Format::from_str(args.next_string()?.as_str())?;
                }
                _ => break,
            };
        } else {
            break;
        }
    }

    let key = ctx.open_key_writable(&key);
    let current = key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)?;

    match (current, set_option) {
        (Some(_), SetOptions::NotExists) => Ok(().into()),
        (Some(ref mut doc), _) => {
            doc.set_value(&value, &path, format)?;
            REDIS_OK
        }
        (None, SetOptions::AlreadyExists) => Ok(().into()),
        (None, _) => {
            let doc = RedisJSON::from_str(&value, format)?;
            if path == "$" {
                key.set_value(&REDIS_JSON_TYPE, doc)?;
                REDIS_OK
            } else {
                Err("ERR new objects must be created at the root".into())
            }
        }
    }
}

///
/// JSON.GET <key>
///         [INDENT indentation-string]
///         [NEWLINE line-break-string]
///         [SPACE space-string]
///         [NOESCAPE]
///         [path ...]
///
/// TODO add support for multi path
fn json_get(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;

    let mut paths: Vec<String> = vec![];
    let mut first_loop = true;
    let mut format = Format::JSON;
    loop {
        let arg = match args.next_string() {
            Ok(s) => s,
            Err(_) => {
                // path is optional -> no path found on the first loop we use root "$"
                if first_loop {
                    paths.push("$".to_owned());
                }
                break;
            }
        };
        first_loop = false;

        match arg.as_str() {
            "INDENT" => {
                args.next();
            } // TODO add support
            "NEWLINE" => {
                args.next();
            } // TODO add support
            "SPACE" => {
                args.next();
            } // TODO add support
            "NOESCAPE" => {
                continue;
            } // TODO add support
            "FORMAT" => {
                format = Format::from_str(args.next_string()?.as_str())?;
            }
            _ => {
                paths.push(backward_path(arg));
            }
        };
    }

    let key = ctx.open_key_writable(&key);
    let value = match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => if paths.len() == 1 {
            doc.to_string(&paths[0], format)?
        } else {
            // can't be smaller than 1
            doc.to_json(&mut paths)?
        }
        .into(),
        None => ().into(),
    };

    Ok(value)
}

///
/// JSON.MGET <key> [key ...] <path>
///
fn json_mget(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    if let Some(path) = args.last() {
        let path = backward_path(path.to_string());
        let mut results: Vec<Option<String>> = Vec::with_capacity(args.len() - 2);
        for key in &args[1..args.len() - 1] {
            let redis_key = ctx.open_key(&key);
            match redis_key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
                Some(doc) => {
                    let result = doc.to_string(&path, Format::JSON)?;
                    results.push(Some(result));
                }
                None => {
                    results.push(None);
                }
            };
        }
        Ok(results.into())
    } else {
        Err(RedisError::WrongArity)
    }
}

///
/// JSON.STRLEN <key> [path]
///
fn json_str_len(ctx: &Context, args: Vec<String>) -> RedisResult {
    json_len(ctx, args, |doc, path| doc.str_len(path))
}

///
/// JSON.TYPE <key> [path]
///
fn json_type(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);

    let key = ctx.open_key(&key);

    let value = match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => doc.get_type(&path)?.into(),
        None => ().into(),
    };

    Ok(value)
}

///
/// JSON.NUMINCRBY <key> <path> <number>
///
fn json_num_incrby(ctx: &Context, args: Vec<String>) -> RedisResult {
    json_num_op(ctx, args, |num1, num2| num1 + num2)
}

///
/// JSON.NUMMULTBY <key> <path> <number>
///
fn json_num_multby(ctx: &Context, args: Vec<String>) -> RedisResult {
    json_num_op(ctx, args, |num1, num2| num1 * num2)
}

///
/// JSON.NUMPOWBY <key> <path> <number>
///
fn json_num_powby(ctx: &Context, args: Vec<String>) -> RedisResult {
    json_num_op(ctx, args, |num1, num2| num1.powf(num2))
}

fn json_num_op<F>(ctx: &Context, args: Vec<String>, fun: F) -> RedisResult
where
    F: Fn(f64, f64) -> f64,
{
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);
    let number: f64 = args.next_string()?.parse()?;

    let key = ctx.open_key_writable(&key);

    match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => Ok(doc
            .value_op(&path, |value| {
                if let Value::Number(curr) = value {
                    if let Some(curr_value) = curr.as_f64() {
                        let res = fun(curr_value, number);
                        if let Some(new_value) = Number::from_f64(res) {
                            Ok(Value::Number(new_value))
                        } else {
                            Err("ERR can not represent result as Number".into())
                        }
                    } else {
                        Err("ERR can not convert current value as f64".into())
                    }
                } else {
                    Err(format!(
                        "ERR wrong type of path value - expected a number but found {}",
                        RedisJSON::value_name(&value)
                    )
                    .into())
                }
            })?
            .into()),
        None => Err("ERR could not perform this operation on a key that doesn't exist".into()),
    }
}

///
/// JSON.STRAPPEND <key> [path] <json-string>
///
fn json_str_append(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let mut path = "$".to_string();
    let mut json = args.next_string()?;

    // path is optional
    if let Ok(val) = args.next_string() {
        path = backward_path(json);
        json = val;
    }

    let key = ctx.open_key_writable(&key);

    match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => {
            let mut res = 0;
            doc.value_op(&path, |value| {
                if let Value::String(curr) = value {
                    let mut curr_clone = curr.clone();
                    curr_clone.push_str(json.as_str());
                    res = curr_clone.len();
                    Ok(Value::String(curr_clone))
                } else {
                    Err(format!(
                        "ERR wrong type of path value - expected a string but found {}",
                        RedisJSON::value_name(&value)
                    )
                    .into())
                }
            })?;
            Ok(res.into())
        }
        None => Err("ERR could not perform this operation on a key that doesn't exist".into()),
    }
}

///
/// JSON.ARRAPPEND <key> <path> <json> [json ...]
///
fn json_arr_append(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);
    let mut json = args.next_string()?;

    let key = ctx.open_key_writable(&key);

    match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => {
            let mut res = 0;
            doc.value_op(&path, |value| {
                if let Value::Array(curr) = value {
                    let mut curr_clone = curr.clone();
                    loop {
                        let value = serde_json::from_str(json.as_str())?;
                        curr_clone.push(value);

                        if let Ok(val) = args.next_string() {
                            json = val;
                        } else {
                            break;
                        }
                    }
                    res = curr_clone.len();
                    Ok(Value::Array(curr_clone))
                } else {
                    Err(format!(
                        "ERR wrong type of path value - expected a string but found {}",
                        RedisJSON::value_name(&value)
                    )
                    .into())
                }
            })?;
            Ok(res.into())
        }
        None => Err("ERR could not perform this operation on a key that doesn't exist".into()),
    }
}

///
/// JSON.ARRINDEX <key> <path> <json-scalar> [start [stop]]
///
/// scalar - number, string, Boolean (true or false), or null
///
fn json_arr_index(ctx: &Context, args: Vec<String>) -> RedisResult {
    let args_len = args.len();
    if args_len < 4 {
        return Err(RedisError::WrongArity);
    }

    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);
    let json_scalar = args.next_string()?;

    let start: usize = if args_len >= 5 {
        args.next_string()?.parse()?
    } else {
        0
    };

    let end: usize = if args_len >= 6 {
        args.next_string()?.parse()?
    } else {
        usize::MAX
    };

    let key = ctx.open_key(&key);
    let index: i64 = match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => doc.arr_index(&path, &json_scalar, start, end)?,
        None => -1,
    };

    Ok(index.into())
}

///
/// JSON.ARRINSERT <key> <path> <index> <json> [json ...]
///
fn json_arr_insert(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);
    let mut index: i64 = args.next_string()?.parse()?;
    let mut json = args.next_string()?;

    let key = ctx.open_key_writable(&key);

    match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => Ok(doc
            .value_op(&path, |value| {
                if let Value::Array(curr) = value {
                    let len = curr.len() as i64;
                    if i64::abs(index) >= len {
                        Err("ERR index out of bounds".into())
                    } else {
                        if index < 0 {
                            index = len + index;
                        }

                        let mut res = curr.clone();

                        loop {
                            let value = serde_json::from_str(json.as_str())?;
                            res.insert(index as usize, value);
                            index = index + 1;
                            // path is optional
                            if let Ok(val) = args.next_string() {
                                json = val;
                            } else {
                                break;
                            }
                        }
                        Ok(Value::Array(res))
                    }
                } else {
                    Err(format!(
                        "ERR wrong type of path value - expected a string but found {}",
                        RedisJSON::value_name(&value)
                    )
                    .into())
                }
            })?
            .into()),
        None => Err("ERR could not perform this operation on a key that doesn't exist".into()),
    }
}

///
/// JSON.ARRLEN <key> [path]
///
fn json_arr_len(ctx: &Context, args: Vec<String>) -> RedisResult {
    json_len(ctx, args, |doc, path| doc.arr_len(path))
}

///
/// JSON.ARRPOP <key> [path [index]]
///
fn json_arr_pop(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;
    let (path, mut index): (String, i64) = if let Ok(mut p) = args.next_string() {
        p = backward_path(p);
        if let Ok(i) = args.next_string() {
            (p, i.parse()?)
        } else {
            (p, i64::MAX)
        }
    } else {
        ("$".to_string(), i64::MAX)
    };

    let key = ctx.open_key_writable(&key);

    match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => {
            let mut res = Value::Null;
            doc.value_op(&path, |value| {
                if let Value::Array(curr) = value {
                    index = cmp::min(index, curr.len() as i64 - 1);
                    if index < 0 {
                        index = curr.len() as i64 + index;
                    }
                    if index >= curr.len() as i64 || index < 0 {
                        Err("ERR index out of bounds".into())
                    } else {
                        let mut curr_clone = curr.clone();
                        res = curr_clone.remove(index as usize);
                        Ok(Value::Array(curr_clone))
                    }
                } else {
                    Err(format!(
                        "ERR wrong type of path value - expected a array but found {}",
                        RedisJSON::value_name(&value)
                    )
                    .into())
                }
            })?;
            Ok(res.to_string().into())
        }
        None => Err("ERR could not perform this operation on a key that doesn't exist".into()),
    }
}

///
/// JSON.ARRTRIM <key> <path> <start> <stop>
///
fn json_arr_trim(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);
    let mut start: usize = args.next_string()?.parse()?;
    let mut stop: usize = args.next_string()?.parse()?;

    let key = ctx.open_key_writable(&key);

    match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => Ok(doc
            .value_op(&path, |value| {
                if let Value::Array(curr) = value {
                    start = cmp::max(start, 0);
                    stop = cmp::min(stop, curr.len() - 1);
                    start = cmp::min(stop, start);
                    let res = &curr[start..stop];
                    Ok(Value::Array(res.to_vec()))
                } else {
                    Err(format!(
                        "ERR wrong type of path value - expected a array but found {}",
                        RedisJSON::value_name(&value)
                    )
                    .into())
                }
            })?
            .into()),
        None => Err("ERR could not perform this operation on a key that doesn't exist".into()),
    }
}

///
/// JSON.OBJKEYS <key> [path]
///
fn json_obj_keys(ctx: &Context, args: Vec<String>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);

    let key = ctx.open_key(&key);

    let value = match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => doc.obj_keys(&path)?.into(),
        None => ().into(),
    };

    Ok(value)
}

///
/// JSON.OBJLEN <key> [path]
///
fn json_obj_len(ctx: &Context, args: Vec<String>) -> RedisResult {
    json_len(ctx, args, |doc, path| doc.obj_len(path))
}

///
/// JSON.DEBUG <subcommand & arguments>
///
/// subcommands:
/// MEMORY <key> [path]
/// HELP
///
fn json_debug(_ctx: &Context, _args: Vec<String>) -> RedisResult {
    Err("Command was not implemented".into())
}

///
/// JSON.RESP <key> [path]
///
fn json_resp(_ctx: &Context, _args: Vec<String>) -> RedisResult {
    Err("Command was not implemented".into())
}

fn json_len<F: Fn(&RedisJSON, &String) -> Result<usize, Error>>(
    ctx: &Context,
    args: Vec<String>,
    fun: F,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_string()?;
    let path = backward_path(args.next_string()?);

    let key = ctx.open_key(&key);
    let length = match key.get_value::<RedisJSON>(&REDIS_JSON_TYPE)? {
        Some(doc) => fun(&doc, &path)?.into(),
        None => ().into(),
    };

    Ok(length)
}

fn json_cache_info(_ctx: &Context, _args: Vec<String>) -> RedisResult {
    Err("Command was not implemented".into())
}

fn json_cache_init(_ctx: &Context, _args: Vec<String>) -> RedisResult {
    Err("Command was not implemented".into())
}
//////////////////////////////////////////////////////

redis_module! {
    name: "redisjson",
    version: 1,
    data_types: [
        REDIS_JSON_TYPE,
    ],
    commands: [
        ["json.del", json_del, "write"],
        ["json.get", json_get, ""],
        ["json.mget", json_mget, ""],
        ["json.set", json_set, "write"],
        ["json.type", json_type, ""],
        ["json.numincrby", json_num_incrby, "write"],
        ["json.nummultby", json_num_multby, "write"],
        ["json.numpowby", json_num_powby, "write"],
        ["json.strappend", json_str_append, "write"],
        ["json.strlen", json_str_len, ""],
        ["json.arrappend", json_arr_append, "write"],
        ["json.arrindex", json_arr_index, ""],
        ["json.arrinsert", json_arr_insert, "write"],
        ["json.arrlen", json_arr_len, ""],
        ["json.arrpop", json_arr_pop, "write"],
        ["json.arrtrim", json_arr_trim, "write"],
        ["json.objkeys", json_obj_keys, ""],
        ["json.objlen", json_obj_len, ""],
        ["json.debug", json_debug, ""],
        ["json.forget", json_del, "write"],
        ["json.resp", json_resp, ""],
        ["json._cacheinfo", json_cache_info, ""],
        ["json._cacheinit", json_cache_init, "write"],
    ],
}
