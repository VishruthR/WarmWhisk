use std::collections::HashMap;

fn main_fn(params: &HashMap<String, String>) -> HashMap<String, String> {
    let name = params.get("name").map(|s| s.as_str()).unwrap_or("");
    let mut result = HashMap::new();
    result.insert("message".to_string(), format!("Hello {}", name));
    result
}