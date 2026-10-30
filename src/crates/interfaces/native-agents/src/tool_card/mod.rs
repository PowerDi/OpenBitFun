//! Normalize agent tool input and results into the shapes OpenBitFun tool cards read.
mod tool_params;
mod tool_result;

pub fn normalize_tool_params(tool_name: &str, params: serde_json::Value) -> serde_json::Value {
    tool_params::normalize_tool_params(tool_name, params)
}

/// Translate a completed tool's result into the shape its native card reads.
/// Results the card already understands, and results this cannot parse, are
/// returned unchanged.
pub fn normalize_tool_result(tool_name: &str, result: serde_json::Value) -> serde_json::Value {
    tool_result::normalize_tool_result(tool_name, result)
}
