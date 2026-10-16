use crate::model::{ClusterProfile, KafkaRecord, RecordHeader};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveHeader {
    key: String,
    value_base64: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveRecord {
    format: String,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: Option<i64>,
    key_base64: Option<String>,
    value_base64: Option<String>,
    headers: Vec<ArchiveHeader>,
}
impl From<&KafkaRecord> for ArchiveRecord {
    fn from(r: &KafkaRecord) -> Self {
        Self {
            format: "kafunekafka.record.v1".into(),
            topic: r.topic.clone(),
            partition: r.partition,
            offset: r.offset,
            timestamp: r.timestamp,
            key_base64: r.key.as_ref().map(|b| STANDARD.encode(b)),
            value_base64: r.value.as_ref().map(|b| STANDARD.encode(b)),
            headers: r
                .headers
                .iter()
                .map(|h| ArchiveHeader {
                    key: h.key.clone(),
                    value_base64: h.value.as_ref().map(|b| STANDARD.encode(b)),
                })
                .collect(),
        }
    }
}
impl TryFrom<ArchiveRecord> for KafkaRecord {
    type Error = anyhow::Error;
    fn try_from(r: ArchiveRecord) -> Result<Self> {
        ensure!(
            r.format == "kafunekafka.record.v1",
            "Formato de arquivo desconhecido: {}",
            r.format
        );
        ensure!(
            r.partition >= 0 && r.offset >= 0,
            "Particao e offset de origem invalidos"
        );
        let decode = |s: Option<String>| -> Result<Option<Vec<u8>>> {
            s.map(|v| STANDARD.decode(v).context("Base64 invalido no arquivo"))
                .transpose()
        };
        Ok(Self {
            topic: r.topic,
            partition: r.partition,
            offset: r.offset,
            timestamp: r.timestamp,
            key: decode(r.key_base64)?,
            value: decode(r.value_base64)?,
            headers: r
                .headers
                .into_iter()
                .map(|h| {
                    Ok(RecordHeader {
                        key: h.key,
                        value: decode(h.value_base64)?,
                    })
                })
                .collect::<Result<_>>()?,
        })
    }
}

pub fn write_record(writer: &mut impl Write, record: &KafkaRecord) -> Result<()> {
    serde_json::to_writer(&mut *writer, &ArchiveRecord::from(record))?;
    writer.write_all(b"\n")?;
    Ok(())
}
pub fn parse_record(line: &str) -> Result<KafkaRecord> {
    KafkaRecord::try_from(
        serde_json::from_str::<ArchiveRecord>(line).context("Linha JSON invalida")?,
    )
}

pub fn visit_archive(
    path: &Path,
    mut visit: impl FnMut(KafkaRecord) -> Result<bool>,
) -> Result<usize> {
    let mut reader = BufReader::with_capacity(
        64 * 1024,
        File::open(path).context("Nao foi possivel abrir o arquivo")?,
    );
    let mut count = 0;
    let mut line = String::new();
    let mut index = 0;
    loop {
        line.clear();
        // Bound allocation before reading an untrusted line, and reuse its capacity.
        if Read::take(&mut reader, 64 * 1024 * 1024 + 2).read_line(&mut line)? == 0 {
            break;
        }
        index += 1;
        let line = line.trim_end_matches(['\r', '\n']);
        ensure!(
            line.len() <= 64 * 1024 * 1024,
            "Linha {} excede 64 MiB",
            index
        );
        if line.trim().is_empty() {
            continue;
        }
        let record = parse_record(line).with_context(|| format!("Erro na linha {index}"))?;
        count += 1;
        if !visit(record)? {
            break;
        }
    }
    Ok(count)
}

pub fn save_records(path: &Path, records: &[KafkaRecord]) -> Result<()> {
    let mut output = AtomicArchive::new(path)?;
    for record in records {
        output.write(record)?;
    }
    output.finish()?;
    Ok(())
}
pub struct AtomicArchive {
    temp: BufWriter<tempfile::NamedTempFile>,
    destination: PathBuf,
}
impl AtomicArchive {
    pub fn new(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        ensure!(parent.is_dir(), "Pasta de destino nao existe");
        Ok(Self {
            temp: BufWriter::with_capacity(256 * 1024, tempfile::NamedTempFile::new_in(parent)?),
            destination: path.to_owned(),
        })
    }
    pub fn write(&mut self, record: &KafkaRecord) -> Result<()> {
        write_record(&mut self.temp, record)
    }
    pub fn finish(mut self) -> Result<PathBuf> {
        self.temp.flush()?;
        self.temp.get_ref().as_file().sync_all()?;
        self.temp
            .into_inner()
            .map_err(|e| e.into_error())?
            .persist(&self.destination)
            .map_err(|e| e.error)?;
        Ok(self.destination)
    }
    pub fn preserve_partial(mut self) -> Result<PathBuf> {
        let partial = self.destination.with_file_name(format!(
            "{}.{}.partial",
            self.destination
                .file_name()
                .unwrap_or_default()
                .to_string_lossy(),
            uuid::Uuid::new_v4().simple()
        ));
        self.temp.flush()?;
        self.temp.get_ref().as_file().sync_all()?;
        self.temp
            .into_inner()
            .map_err(|e| e.into_error())?
            .persist(&partial)
            .map_err(|e| e.error)?;
        Ok(partial)
    }
}

pub fn save_raw(path: &Path, bytes: Option<&[u8]>) -> Result<()> {
    let bytes = bytes.context("Valor null: use o arquivo JSONL para preservar um tombstone")?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

pub fn profile_path() -> Result<PathBuf> {
    let dirs = directories::ProjectDirs::from("dev", "kafunekafka", "kafunekafka")
        .context("Pasta de configuracao indisponivel")?;
    Ok(dirs.config_dir().join("clusters.json"))
}
pub fn load_profiles(path: &Path) -> Result<Vec<ClusterProfile>> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let profiles =
        serde_json::from_slice(&fs::read(path)?).context("Configuracao de clusters invalida")?;
    Ok(profiles)
}
pub fn save_profiles(path: &Path, profiles: &[ClusterProfile]) -> Result<()> {
    for profile in profiles {
        for name in profile.properties.keys() {
            if name.to_ascii_lowercase().contains("password")
                || name.to_ascii_lowercase().contains("secret")
            {
                bail!(
                    "Use o campo de senha ou uma variavel de ambiente; propriedades sensiveis nao sao salvas"
                );
            }
        }
    }
    fs::create_dir_all(path.parent().context("Caminho de configuracao invalido")?)?;
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    serde_json::to_writer_pretty(temp.as_file_mut(), profiles)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip_preserves_binary_null_empty_and_duplicate_headers() {
        let records = vec![
            KafkaRecord {
                topic: "a".into(),
                partition: 2,
                offset: 7,
                timestamp: Some(123),
                key: Some(vec![0, 255]),
                value: None,
                headers: vec![
                    RecordHeader {
                        key: "x".into(),
                        value: None,
                    },
                    RecordHeader {
                        key: "x".into(),
                        value: Some(vec![]),
                    },
                ],
            },
            KafkaRecord {
                topic: "b".into(),
                partition: 0,
                offset: 9,
                timestamp: None,
                key: None,
                value: Some(vec![]),
                headers: vec![],
            },
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.jsonl");
        save_records(&path, &records).unwrap();
        let mut restored = vec![];
        visit_archive(&path, |r| {
            restored.push(r);
            Ok(true)
        })
        .unwrap();
        assert_eq!(records, restored);
    }
    #[test]
    fn passwords_are_not_persisted() {
        let p = ClusterProfile {
            password: "never-save-me".into(),
            registry_password: "also-secret".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(!json.contains("never-save-me"));
        assert!(!json.contains("also-secret"));
    }
    #[test]
    fn invalid_archive_rejected_before_publish() {
        assert!(parse_record("{\"format\":\"unknown\"}").is_err());
    }
}
