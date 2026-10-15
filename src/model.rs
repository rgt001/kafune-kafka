use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterProfile {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub username: String,
    #[serde(skip)]
    pub password: String,
    pub password_env: String,
    pub ca_path: String,
    pub certificate_path: String,
    pub private_key_path: String,
    pub registry_url: String,
    pub registry_username: String,
    #[serde(skip)]
    pub registry_password: String,
    pub registry_password_env: String,
    pub avro_schema: String,
    pub properties: BTreeMap<String, String>,
}

impl Default for ClusterProfile {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name: "Local Kafka".into(),
            bootstrap_servers: "localhost:19092".into(),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: "PLAIN".into(),
            username: String::new(),
            password: String::new(),
            password_env: String::new(),
            ca_path: String::new(),
            certificate_path: String::new(),
            private_key_path: String::new(),
            registry_url: "http://localhost:18081".into(),
            registry_username: String::new(),
            registry_password: String::new(),
            registry_password_env: String::new(),
            avro_schema: String::new(),
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterSnapshot {
    pub brokers: Vec<BrokerInfo>,
    pub topics: Vec<TopicInfo>,
    pub groups: Vec<GroupInfo>,
    pub warnings: Vec<String>,
}
#[derive(Clone, Debug)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}
#[derive(Clone, Debug)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
}
#[derive(Clone, Debug)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub in_sync: Vec<i32>,
}
#[derive(Clone, Debug)]
pub struct GroupInfo {
    pub name: String,
    pub state: String,
    pub protocol: String,
    pub members: Vec<MemberInfo>,
}
#[derive(Clone, Debug)]
pub struct MemberInfo {
    pub id: String,
    pub client_id: String,
    pub host: String,
}
#[derive(Clone, Debug)]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub committed: Option<i64>,
    pub low: i64,
    pub high: i64,
    pub lag: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StartPosition {
    Earliest,
    Latest(usize),
    Offset(i64),
    Timestamp(i64),
}
#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub topics: Vec<String>,
    pub partitions: Vec<i32>,
    pub start: StartPosition,
    pub max_messages: usize,
    pub max_scanned: usize,
    pub timeout_seconds: u64,
}
impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            topics: vec![],
            partitions: vec![],
            start: StartPosition::Latest(100),
            max_messages: 1000,
            max_scanned: 100_000,
            timeout_seconds: 30,
        }
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct OperationReport {
    pub scanned: usize,
    pub matched: usize,
    pub delivered: usize,
    pub cancelled: bool,
    pub complete: bool,
    pub message: String,
    pub destination_offsets: Vec<(String, i32, i64)>,
}
#[derive(Clone, Debug)]
pub struct ScanResult {
    pub records: Vec<KafkaRecord>,
    pub report: OperationReport,
}
#[derive(Clone, Debug, Default)]
pub struct Progress {
    pub scanned: usize,
    pub matched: usize,
    pub delivered: usize,
    pub phase: String,
}
#[derive(Clone, Debug)]
pub struct SearchOptions {
    pub pattern: String,
    pub regex: bool,
    pub case_sensitive: bool,
    pub keys: bool,
    pub values: bool,
    pub headers: bool,
}
impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            pattern: String::new(),
            regex: false,
            case_sensitive: false,
            keys: true,
            values: true,
            headers: false,
        }
    }
}
#[derive(Clone, Debug)]
pub struct ProduceRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
}
impl From<&KafkaRecord> for ProduceRecord {
    fn from(record: &KafkaRecord) -> Self {
        Self {
            topic: record.topic.clone(),
            partition: Some(record.partition),
            timestamp: record.timestamp,
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record.headers.clone(),
        }
    }
}

impl From<KafkaRecord> for ProduceRecord {
    fn from(record: KafkaRecord) -> Self {
        Self {
            topic: record.topic,
            partition: Some(record.partition),
            timestamp: record.timestamp,
            key: record.key,
            value: record.value,
            headers: record.headers,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplayOptions {
    pub destination_topic: String,
    pub preserve_partition: bool,
    pub preserve_timestamp: bool,
}
