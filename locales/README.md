# Interface translations

`catalog.json` is embedded in the executable. Each entry uses the original Portuguese message as its key and contains two translations, in order: English and Russian. Portuguese remains the default for existing users.

Translate the complete message. Preserve all numbered placeholders (`{0}`, `{1}`, etc.); their order may change. Do not translate protocol names, Kafka payloads, topic names, archive fields or user-entered data. Call `tr` for static text and `tf!` for templates, before inserting user data.

Each UI frame and background job has a scoped language. A job keeps the language selected when it started; the scope restores the prior language after errors or panics. Operation history retains the language used when each entry was recorded. Native file-dialog buttons follow the operating system.

The language selector writes only `settings.json`, beside the existing cluster-profile file. `KAFUNEKAFKA_LANGUAGE=pt-BR`, `en` or `ru` temporarily overrides the saved preference. `KAFUNEKAFKA_CONFIG` can isolate both files for testing. The catalog is checked for missing messages and mismatched placeholders by `cargo test --locked --lib`.
