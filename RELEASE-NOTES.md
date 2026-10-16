# kafunekafka 0.4.0

English and Russian interface support alongside Portuguese, under the MIT licence.

## Languages

- Choose **Português**, **English** or **Русский** in the top-right language menu, without restarting.
- The selection is remembered in `settings.json`, beside the existing cluster-profile file. Existing connections are preserved; Portuguese remains the default.
- Translated navigation, forms, table headers, application errors, validation and operation progress. Kafka payloads and user-entered names remain intact.
- Worker jobs use the selected language. Operation history retains each entry's original language; native file-dialog buttons follow the operating system.
- `KAFUNEKAFKA_LANGUAGE=pt-BR`, `en` or `ru` provides a temporary override.
- Russian documentation and an embedded catalog checked for missing messages and mismatched placeholders.

## Download for Windows

Download **`kafunekafka-v0.4.0-windows-x64.zip`**, extract the entire ZIP and run **`kafunekafka.exe`**. Keep its DLLs alongside the executable. Rust, Java, Offset Explorer and Docker are not required to run the app.

The matching `.sha256` asset lets you verify the download. Application and dependency licences are included. Local cluster profiles, credentials and development tools are excluded.

**Linux x64:** `kafunekafka-v0.4.0-linux-x64.tar.gz` contains the native executable, built and tested on Ubuntu 22.04 through WSL (glibc 2.35). Extract it and run `./kafunekafka/kafunekafka` from an X11/Wayland desktop with OpenGL. Other distributions have not been exercised.

## Included

- Clusters, brokers, topics, partitions and consumer groups.
- Message inspection, consumer offsets, JSON/XML/Avro formatting.
- Publishing, editable resend, text/regex search.
- Import/export archives and replay across topics and clusters.
- A permanent worker and reused readers to reduce read latency.
- Native Windows and Linux builds; Portuguese, English and Russian interfaces.

Message archives continue to use `kafunekafka.record.v1`, preserving compatibility with version 0.3.0.

## Validation

Unit tests, clippy and real-Kafka integration are run on Windows and Linux before publication, including locale isolation, preference persistence, placeholder integrity, payload preservation, Avro Schema Registry references, consumer offsets, cross-cluster replay, cancellation and reused-reader recovery. Native GUI captures check English and Russian rendering. PLAINTEXT and SASL/PLAIN are exercised locally; TLS/SCRAM and production clusters have not been exercised.

Historical local GUI measurements from 2026-10-14, before the branding change, reduced the median for 12,000 messages from 185.7 ms to 62.8 ms across 45 reads per version. See `PERFORMANCE-WINDOWS-GUI.md` for methods and limits. These are earlier measurements, not a new performance benchmark of this release.
