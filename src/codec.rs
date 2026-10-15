use crate::model::ClusterProfile;
use anyhow::{Context, Result, bail, ensure};
use apache_avro::{Schema, types::Value};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisplayFormat {
    Auto,
    Text,
    Json,
    Xml,
    Avro,
    AvroRaw,
    Hex,
    Base64,
}
impl DisplayFormat {
    pub const ALL: [Self; 8] = [
        Self::Auto,
        Self::Text,
        Self::Json,
        Self::Xml,
        Self::Avro,
        Self::AvroRaw,
        Self::Hex,
        Self::Base64,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::Text => "Texto",
            Self::Json => "JSON",
            Self::Xml => "XML",
            Self::Avro => "Avro",
            Self::AvroRaw => "Avro bruto",
            Self::Hex => "Hex",
            Self::Base64 => "Base64",
        }
    }
}
pub struct Decoder {
    profile: ClusterProfile,
    schemas: HashMap<u32, Schema>,
    dependencies: HashMap<u32, Vec<Schema>>,
    http: Option<reqwest::blocking::Client>,
}
impl Decoder {
    pub fn new(profile: &ClusterProfile) -> Result<Self> {
        Ok(Self {
            profile: profile.clone(),
            schemas: HashMap::new(),
            dependencies: HashMap::new(),
            http: None,
        })
    }
    fn http(&mut self) -> Result<&reqwest::blocking::Client> {
        if self.http.is_none() {
            let mut builder =
                reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(8));
            if !self.profile.ca_path.is_empty() {
                builder = builder.add_root_certificate(reqwest::Certificate::from_pem(
                    &std::fs::read(&self.profile.ca_path)?,
                )?);
            }
            self.http = Some(builder.build()?);
        }
        Ok(self.http.as_ref().unwrap())
    }
    pub fn display(&mut self, bytes: Option<&[u8]>, format: DisplayFormat) -> Result<String> {
        let Some(bytes) = bytes else {
            return Ok("null (tombstone)".into());
        };
        if format == DisplayFormat::Hex {
            return Ok(hex::encode(bytes));
        }
        if format == DisplayFormat::Base64 {
            return Ok(STANDARD.encode(bytes));
        }
        if format == DisplayFormat::AvroRaw {
            ensure!(
                !self.profile.avro_schema.trim().is_empty(),
                "Configure o schema local para Avro bruto"
            );
            let schema = Schema::parse_str(&self.profile.avro_schema)?;
            let mut input = Cursor::new(bytes);
            let value = apache_avro::from_avro_datum(&schema, &mut input, None)?;
            ensure!(
                input.position() == bytes.len() as u64,
                "Bytes adicionais depois do datum Avro bruto"
            );
            return Ok(serde_json::to_string_pretty(&avro_json(value)?)?);
        }
        if format == DisplayFormat::Avro
            || (format == DisplayFormat::Auto
                && (bytes.starts_with(b"Obj\x01") || bytes.len() >= 5 && bytes[0] == 0))
        {
            return self.avro(bytes);
        }
        let text = match std::str::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) if format == DisplayFormat::Auto => {
                return Ok(format!(
                    "[binario, {} bytes]\n{}",
                    bytes.len(),
                    hex::encode(bytes)
                ));
            }
            Err(e) => return Err(e).context("Dados nao sao UTF-8; selecione Hex ou Base64"),
        };
        if format == DisplayFormat::Json
            || format == DisplayFormat::Auto
                && (text.trim_start().starts_with('{') || text.trim_start().starts_with('['))
        {
            match serde_json::from_str::<serde_json::Value>(text) {
                Ok(json) => return Ok(serde_json::to_string_pretty(&json)?),
                Err(e) if format == DisplayFormat::Json => return Err(e).context("JSON invalido"),
                _ => (),
            }
        }
        if format == DisplayFormat::Xml
            || format == DisplayFormat::Auto && text.trim_start().starts_with('<')
        {
            match pretty_xml(text) {
                Ok(xml) => return Ok(xml),
                Err(e) if format == DisplayFormat::Xml => return Err(e),
                _ => (),
            }
        }
        Ok(text.into())
    }
    fn avro(&mut self, bytes: &[u8]) -> Result<String> {
        if bytes.starts_with(b"Obj\x01") {
            let reader = apache_avro::Reader::new(bytes)?;
            let values: Vec<serde_json::Value> =
                reader.map(|v| avro_json(v?)).collect::<Result<_>>()?;
            return Ok(serde_json::to_string_pretty(&if values.len() == 1 {
                values.into_iter().next().unwrap()
            } else {
                serde_json::Value::Array(values)
            })?);
        }
        let (schema, payload, dependencies) = if bytes.len() >= 5 && bytes[0] == 0 {
            let id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
            (
                self.schema(id)?,
                &bytes[5..],
                self.dependencies.get(&id).cloned().unwrap_or_default(),
            )
        } else {
            ensure!(
                !self.profile.avro_schema.trim().is_empty(),
                "Avro binario exige um schema local ou Schema Registry"
            );
            (Schema::parse_str(&self.profile.avro_schema)?, bytes, vec![])
        };
        let mut payload = Cursor::new(payload);
        let value = apache_avro::from_avro_datum_schemata(
            &schema,
            dependencies.iter().collect(),
            &mut payload,
            None,
        )
        .context("Payload Avro invalido")?;
        ensure!(
            payload.position() == payload.get_ref().len() as u64,
            "Bytes adicionais depois do datum Avro"
        );
        Ok(serde_json::to_string_pretty(&avro_json(value)?)?)
    }
    pub fn schema(&mut self, id: u32) -> Result<Schema> {
        if let Some(schema) = self.schemas.get(&id) {
            return Ok(schema.clone());
        }
        ensure!(
            !self.profile.registry_url.trim().is_empty(),
            "Mensagem usa Schema Registry (id {id}); configure a URL do registry"
        );
        let response = self.registry_get(&format!("schemas/ids/{id}"))?;
        let mut texts = vec![];
        let mut seen = HashSet::new();
        self.collect_references(&response, &mut seen, &mut texts)?;
        let text = response
            .get("schema")
            .and_then(|v| v.as_str())
            .context("Resposta do registry sem schema")?;
        let schema = if texts.is_empty() {
            Schema::parse_str(text)?
        } else {
            texts.push(text.to_owned());
            let parsed = Schema::parse_list(&texts)?;
            let root = parsed.last().context("Schema Avro vazio")?.clone();
            self.dependencies.insert(id, parsed);
            root
        };
        self.schemas.insert(id, schema.clone());
        Ok(schema)
    }
    fn registry_get(&mut self, path: &str) -> Result<serde_json::Value> {
        let url = format!(
            "{}/{}",
            self.profile.registry_url.trim_end_matches('/'),
            path
        );
        let mut request = self.http()?.get(url);
        if !self.profile.registry_username.is_empty() {
            let password = if !self.profile.registry_password_env.is_empty() {
                std::env::var(&self.profile.registry_password_env)
                    .context("Variavel da senha do registry ausente")?
            } else {
                self.profile.registry_password.clone()
            };
            request = request.basic_auth(&self.profile.registry_username, Some(password));
        }
        let response: serde_json::Value = request
            .send()
            .context("Falha ao acessar Schema Registry")?
            .error_for_status()?
            .json()?;
        if response
            .get("schemaType")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s != "AVRO")
        {
            bail!("Schema do registry nao e Avro");
        }
        Ok(response)
    }
    fn collect_references(
        &mut self,
        response: &serde_json::Value,
        seen: &mut HashSet<(String, i64)>,
        texts: &mut Vec<String>,
    ) -> Result<()> {
        if let Some(references) = response.get("references").and_then(|v| v.as_array()) {
            for reference in references {
                let subject = reference["subject"]
                    .as_str()
                    .context("Referencia Avro sem subject")?;
                let version = reference["version"]
                    .as_i64()
                    .context("Referencia Avro sem version")?;
                if !seen.insert((subject.into(), version)) {
                    continue;
                }
                ensure!(seen.len() <= 64, "Schema Avro tem mais de 64 referencias");
                let mut url = reqwest::Url::parse(&self.profile.registry_url)?;
                url.path_segments_mut()
                    .map_err(|_| anyhow::anyhow!("URL do registry invalida"))?
                    .pop_if_empty()
                    .extend(["subjects", subject, "versions", &version.to_string()]);
                let base = reqwest::Url::parse(&self.profile.registry_url)?;
                let path = url
                    .path()
                    .strip_prefix(base.path().trim_end_matches('/'))
                    .unwrap_or(url.path())
                    .trim_start_matches('/');
                let referenced = self.registry_get(path)?;
                self.collect_references(&referenced, seen, texts)?;
                texts.push(
                    referenced["schema"]
                        .as_str()
                        .context("Referencia Avro sem schema")?
                        .into(),
                );
            }
        }
        Ok(())
    }
    pub fn encode_avro(
        &mut self,
        json: &str,
        schema_text: &str,
        schema_id: Option<u32>,
    ) -> Result<Vec<u8>> {
        let schema = if schema_text.trim().is_empty() {
            self.schema(schema_id.context("Informe schema local ou id do registry")?)?
        } else {
            Schema::parse_str(schema_text)?
        };
        let dependencies = if let Some(id) = schema_id {
            let registered = self.schema(id)?;
            ensure!(
                registered.canonical_form() == schema.canonical_form(),
                "O schema local difere do schema registrado para o id {id}"
            );
            self.dependencies.get(&id).cloned().unwrap_or_default()
        } else {
            vec![]
        };
        let value: Value = serde_json::from_str::<serde_json::Value>(json)?.into();
        let value = value
            .resolve_schemata(&schema, dependencies.iter().collect())
            .context("JSON nao corresponde ao schema Avro")?;
        let payload =
            apache_avro::to_avro_datum_schemata(&schema, dependencies.iter().collect(), value)?;
        let mut output = Vec::new();
        if let Some(id) = schema_id {
            output.push(0);
            output.extend_from_slice(&id.to_be_bytes());
        }
        output.extend_from_slice(&payload);
        Ok(output)
    }
}
fn avro_json(value: Value) -> Result<serde_json::Value> {
    Ok(value.try_into()?)
}
pub fn pretty_xml(text: &str) -> Result<String> {
    roxmltree::Document::parse(text).context("XML invalido")?;
    let mut reader = quick_xml::Reader::from_str(text);
    reader.config_mut().trim_text(false);
    let mut writer = quick_xml::Writer::new_with_indent(Vec::new(), b' ', 2);
    loop {
        let event = reader.read_event()?;
        if matches!(event, quick_xml::events::Event::Eof) {
            break;
        }
        writer.write_event(event)?;
    }
    Ok(String::from_utf8(writer.into_inner())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_formats_do_not_require_registry_tls_setup() {
        let profile = ClusterProfile {
            ca_path: "missing-registry-ca.pem".into(),
            avro_schema: r#""string""#.into(),
            ..Default::default()
        };
        let mut decoder = Decoder::new(&profile).unwrap();
        assert!(
            decoder
                .display(Some(br#"{"ok":true}"#), DisplayFormat::Json)
                .unwrap()
                .contains("true")
        );
        assert!(decoder.display(Some(b"<ok/>"), DisplayFormat::Xml).is_ok());
        let raw = decoder
            .encode_avro(r#""local""#, &profile.avro_schema, None)
            .unwrap();
        assert!(
            decoder
                .display(Some(&raw), DisplayFormat::AvroRaw)
                .unwrap()
                .contains("local")
        );
        assert_eq!(
            decoder.display(Some(&[255]), DisplayFormat::Hex).unwrap(),
            "ff"
        );
        assert!(decoder.schema(1).is_err());
    }
    #[test]
    fn avro_raw_and_container_are_pretty_json() {
        let schema = r#"{"type":"record","name":"Demo","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"#;
        let p = ClusterProfile {
            avro_schema: schema.into(),
            ..Default::default()
        };
        let mut d = Decoder::new(&p).unwrap();
        let raw = d
            .encode_avro(r#"{"name":"Ana","age":31}"#, schema, None)
            .unwrap();
        assert!(
            d.display(Some(&raw), DisplayFormat::Avro)
                .unwrap()
                .contains("\"Ana\"")
        );
        let parsed = Schema::parse_str(schema).unwrap();
        let value = Value::from(serde_json::json!({"name":"Ana","age":31}))
            .resolve(&parsed)
            .unwrap();
        let mut w = apache_avro::Writer::new(&parsed, Vec::new());
        w.append(value).unwrap();
        assert!(
            d.display(Some(&w.into_inner().unwrap()), DisplayFormat::Auto)
                .unwrap()
                .contains("\"age\": 31")
        );
    }
    #[test]
    fn invalid_xml_and_invalid_utf8_are_explicit() {
        assert!(pretty_xml("<root><bad></root>").is_err());
        let mut d = Decoder::new(&ClusterProfile::default()).unwrap();
        assert!(d.display(Some(&[255]), DisplayFormat::Text).is_err());
        assert!(
            d.display(Some(&[255]), DisplayFormat::Auto)
                .unwrap()
                .contains("ff")
        );
        assert_eq!(d.display(Some(b""), DisplayFormat::Text).unwrap(), "");
    }
    #[test]
    fn raw_avro_zero_prefix_and_snappy_container_are_decoded() {
        let schema = r#"{"type":"record","name":"Counter","fields":[{"name":"count","type":"long"},{"name":"text","type":"string"}]}"#;
        let profile = ClusterProfile {
            avro_schema: schema.into(),
            ..Default::default()
        };
        let mut decoder = Decoder::new(&profile).unwrap();
        let raw = decoder
            .encode_avro(r#"{"count":0,"text":"zero prefix"}"#, schema, None)
            .unwrap();
        assert_eq!(raw[0], 0);
        assert!(
            decoder
                .display(Some(&raw), DisplayFormat::AvroRaw)
                .unwrap()
                .contains("zero prefix")
        );
        let parsed = Schema::parse_str(schema).unwrap();
        let value = Value::from(serde_json::json!({"count":0,"text":"snappy"}))
            .resolve(&parsed)
            .unwrap();
        let mut writer =
            apache_avro::Writer::with_codec(&parsed, Vec::new(), apache_avro::Codec::Snappy);
        writer.append(value).unwrap();
        assert!(
            decoder
                .display(Some(&writer.into_inner().unwrap()), DisplayFormat::Auto)
                .unwrap()
                .contains("snappy")
        );
    }
}
