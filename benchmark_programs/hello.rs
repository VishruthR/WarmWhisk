extern crate serde_json;
use serde_json::{Error, Value};


pub fn main(args: Value) -> Result<Value, Error> {
    let name = args["name"].as_str().unwrap_or("stranger");
    let output = serde_json::json!({ "message": format!("Hello, {}", name) });
    serde_json::to_value(output)
}