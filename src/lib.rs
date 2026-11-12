pub mod authority;
pub mod declared;
pub mod engine;
pub mod executor;
pub mod model;
pub mod runner;
pub mod source;
pub mod store;
pub mod worker;

pub use patchweft_control::{file_hash, hash, identity};
use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs_f64()
}

pub fn unique() -> String {
    // OS randomness rather than timestamps; IDs are not authentication credentials.
    use std::io::Read;
    let mut bytes = [0u8; 24];
    fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .expect("OS randomness");
    hash(&bytes)[..32].to_owned()
}
pub mod api;
pub mod client;
pub mod http;
pub mod mcp;

#[cfg(test)]
mod receipt_tests {
    #[test]
    fn persisted_json_preserves_timestamp_values_exactly() {
        // Durable replay must return the same value as the first response.
        // This timestamp exposes a one-ULP change with approximate float parsing.
        let receipt = serde_json::json!({"result": {"created": 1_792_511_574.900_662_7_f64}});
        let stored = serde_json::to_string(&receipt).unwrap();
        let replayed: serde_json::Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(replayed, receipt);
    }
}
