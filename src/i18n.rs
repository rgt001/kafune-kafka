//! UI text is translated before interpolation. Kafka data never passes through a translator.
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Write};
use std::{cell::Cell, collections::BTreeMap, sync::LazyLock};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Language {
    #[default]
    #[serde(rename = "pt-BR")]
    Portuguese,
    #[serde(rename = "en")]
    English,
    #[serde(rename = "ru")]
    Russian,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct UiSettings {
    pub language: Language,
}

type Catalog = BTreeMap<String, [String; 2]>;
static CATALOG: LazyLock<Catalog> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../locales/catalog.json"))
        .expect("the embedded translation catalog must be valid")
});

thread_local! {
    static LANGUAGE: Cell<Language> = const { Cell::new(Language::Portuguese) };
}

impl Language {
    pub const ALL: [Self; 3] = [Self::Portuguese, Self::English, Self::Russian];

    pub fn name(self) -> &'static str {
        match self {
            Self::Portuguese => "Português",
            Self::English => "English",
            Self::Russian => "Русский",
        }
    }

    pub fn from_code(code: &str) -> Option<Self> {
        match code
            .split(['-', '_', '.'])
            .next()?
            .to_ascii_lowercase()
            .as_str()
        {
            "pt" => Some(Self::Portuguese),
            "en" => Some(Self::English),
            "ru" => Some(Self::Russian),
            _ => None,
        }
    }

    /// Each UI frame and worker job has its own locale. Restored even after a panic.
    pub fn with<T>(self, work: impl FnOnce() -> T) -> T {
        struct Restore(Language);
        impl Drop for Restore {
            fn drop(&mut self) {
                LANGUAGE.with(|language| language.set(self.0));
            }
        }
        let _restore = Restore(LANGUAGE.with(|language| language.replace(self)));
        work()
    }

    pub fn text(self, source: &'static str) -> &'static str {
        let index = match self {
            Self::Portuguese => return source,
            Self::English => 0,
            Self::Russian => 1,
        };
        CATALOG
            .get(source)
            .map_or(source, |entry| entry[index].as_str())
    }
}

pub fn tr(source: &'static str) -> &'static str {
    LANGUAGE.with(|language| language.get().text(source))
}

pub fn format(source: &'static str, values: &[&dyn Display]) -> String {
    let template = tr(source);
    let mut output = String::with_capacity(template.len() + values.len() * 8);
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        output.push_str(&rest[..start]);
        rest = &rest[start..];
        if let Some(end) = rest.find('}')
            && let Ok(index) = rest[1..end].parse::<usize>()
            && let Some(value) = values.get(index)
        {
            // Append once: placeholders inside user data are never interpreted.
            write!(&mut output, "{value}").expect("writing to a String is infallible");
            rest = &rest[end + 1..];
        } else {
            output.push('{');
            rest = &rest[1..];
        }
    }
    output.push_str(rest);
    output
}

#[macro_export]
macro_rules! tf {
    ($source:literal $(, $value:expr)+ $(,)?) => {
        $crate::i18n::format($source, &[$(&$value as &dyn std::fmt::Display),+])
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_preserves_all_placeholders_and_covers_source_calls() {
        let placeholders = regex::Regex::new(r"\{[0-9]+\}").unwrap();
        for (source, translations) in CATALOG.iter() {
            let expected: std::collections::BTreeSet<_> =
                placeholders.find_iter(source).map(|m| m.as_str()).collect();
            for text in translations {
                assert!(!text.trim().is_empty(), "{source}");
                let actual: std::collections::BTreeSet<_> =
                    placeholders.find_iter(text).map(|m| m.as_str()).collect();
                assert_eq!(expected, actual, "{source}: {text}");
            }
        }
        let calls = regex::Regex::new(r#"(?:tr|tf!)\(\s*("(?:[^"\\]|\\.)*")"#).unwrap();
        for source in [
            include_str!("ui.rs"),
            include_str!("backend.rs"),
            include_str!("codec.rs"),
            include_str!("storage.rs"),
        ] {
            for call in calls.captures_iter(source) {
                let key: String = serde_json::from_str(&call[1]).unwrap();
                assert!(CATALOG.contains_key(&key), "Missing translation: {key}");
            }
        }
    }

    #[test]
    fn locales_are_scoped_to_the_job_and_restore_after_panics() {
        assert_eq!(tr("Mensagens"), "Mensagens");
        Language::Russian.with(|| {
            assert_eq!(tr("Mensagens"), "Сообщения");
            std::thread::spawn(|| assert_eq!(tr("Mensagens"), "Mensagens"))
                .join()
                .unwrap();
            let _ = std::panic::catch_unwind(|| Language::English.with(|| panic!("fixture")));
            assert_eq!(tr("Mensagens"), "Сообщения");
        });
        assert_eq!(tr("Mensagens"), "Mensagens");
    }

    #[test]
    fn interpolation_preserves_payload_and_locale_codes_are_explicit() {
        Language::Russian.with(|| {
            let data = "Mensagens {0} ${topic} Привет 🦀";
            assert_eq!(
                crate::tf!("Arquivo salvo: {0}", data),
                format!("Файл сохранён: {data}")
            );
        });
        assert_eq!(Language::from_code("ru_RU.UTF-8"), Some(Language::Russian));
        assert_eq!(Language::from_code("en-US"), Some(Language::English));
        assert_eq!(Language::from_code("pt-BR"), Some(Language::Portuguese));
        assert_eq!(Language::from_code("de"), None);
    }
}
