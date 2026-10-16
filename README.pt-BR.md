# kafunekafka

## Stop waiting for better software. Build it.

We're tired of desktop apps that need gigabytes of RAM just to display a few tables.

So we built our own.

**Open Source. AI-Built. Rust-Powered.**

Less bloat. More performance. No excuses.

**The revolution will be compiled.**

---

Aplicativo desktop em Rust para explorar Kafka no Windows e no Linux, inspirado no fluxo de trabalho do [Offset Explorer](https://www.kafkatool.com/).

## Baixar para Windows

[**Download para Windows x64**](https://github.com/rgt001/kafune-kafka/releases/download/v0.3.0/kafunekafka-v0.3.0-windows-x64.zip) · [Linux x64](https://github.com/rgt001/kafune-kafka/releases/download/v0.3.0/kafunekafka-v0.3.0-linux-x64.tar.gz) · [English](README.md)

Na página do repositório no GitHub, abra **Releases** e baixe `kafunekafka-v0.3.0-windows-x64.zip`. Extraia todo o ZIP e execute `kafunekafka.exe`, mantendo as DLLs na mesma pasta. Não precisa instalar Rust, Java ou o Offset Explorer. Docker serve apenas aos testes locais; o aplicativo se conecta ao Kafka pelos bootstrap servers configurados.

Cada release inclui o ZIP, seu arquivo SHA-256, documentação e licenças das dependências. As credenciais e conexões do usuário não fazem parte do download. O código-fonte fica no Git; os executáveis ficam nos assets de cada release.

O pacote Linux foi compilado e testado no Ubuntu 22.04 pelo WSL (glibc 2.35). Extraia o arquivo e execute `./kafunekafka/kafunekafka` em uma sessão gráfica X11/Wayland com OpenGL. Outras distribuições não foram testadas.

## Funções

| # | Função | Onde usar |
|---|---|---|
| 1 | Vários clusters, brokers, tópicos, partições e consumer groups | Barra lateral e aba Cluster |
| 2 | Inspeção de mensagens e inclusão de novas mensagens | Mensagens e Enviar / Reenviar |
| 3 | Offsets comprometidos, início/fim da partição e lag | Consumidores |
| 4 | JSON, XML e Avro formatados | Mensagens, seletor de formato |
| 5 | Salvar uma mensagem ou seu valor binário | Mensagens |
| 6 | Importar mensagens de arquivos locais | Importar / Exportar / Replay |
| 7 | Exportar tópicos, faixas ou a tabela carregada | Importar / Exportar / Replay |
| 8 | Replay para outro tópico, inclusive em outro cluster | Importar / Exportar / Replay |
| 9 | Enviar diretamente para um tópico e partição | Enviar / Reenviar |
| 10 | Reenviar uma mensagem, editando seu conteúdo primeiro | Mensagens → Editar e reenviar |
| 11 | Buscar em vários tópicos e partições, com texto ou regex | Buscar |
| 12 | Executáveis nativos para Windows e Linux | Builds abaixo |

## Uso

1. Cadastre os clusters em **Novo**. Preencha os bootstrap servers e, quando necessário, TLS, SASL/PLAIN ou SCRAM. Credenciais digitadas permanecem na sessão; um nome de variável de ambiente permite reaproveitá-las sem salvar a senha em disco.
2. Clique em **Conectar / atualizar todos**. A interface mostra os clusters cadastrados e mantém as tarefas Kafka fora da thread gráfica.
3. Marque um ou mais tópicos. Partições vazias significam todas; `0,1,2` seleciona partições específicas. Escolha início, últimas N mensagens por partição, offset ou timestamp Unix em milissegundos.
4. Leia ou procure mensagens. Na tabela, selecione a mensagem para ver chave, valor e headers. O seletor **Auto** reconhece JSON, XML, Avro Confluent e arquivos Avro OCF (Null, Deflate e Snappy). Para um datum Avro sem cabeçalho, escolha **Avro bruto** e configure o schema local do cluster.
5. Para reenviar, clique em **Editar e reenviar**, altere os campos desejados e envie. O envio aguarda confirmação do broker e mostra a partição e o novo offset.

Tópicos de destino precisam existir. A versão inicial não inclui criação, exclusão de tópicos ou reset de offsets na interface; esses itens não fazem parte da lista final dos 12 requisitos.

## Importação, exportação e replay

Importação e exportação usam JSONL `kafunekafka.record.v1`. Cada linha preserva a mensagem com chave e valor em Base64, além de tópico/partição/offset de origem, timestamp e todos os headers. `null`, bytes vazios, dados binários e headers repetidos permanecem distintos.

```json
{"format":"kafunekafka.record.v1","topic":"orders","partition":0,"offset":42,"timestamp":1750000000000,"key_base64":null,"value_base64":"eyJpZCI6MX0=","headers":[{"key":"source","value_base64":"dGVzdA=="}]}
```

O importador verifica todo o arquivo e as partições de destino antes do primeiro envio. A opção **Preservar partição** mantém a partição original; desmarcada, o produtor escolhe a partição. **Preservar timestamp** solicita o timestamp original. O broker atribui novos offsets e pode aplicar sua política de timestamps. Replay preserva bytes e headers sem converter JSON, XML ou Avro. Para Avro em outro cluster, configure um registry que resolva os mesmos IDs de schema da origem.

A leitura fixa os limites superiores das partições ao iniciar. Um produtor ativo não estende indefinidamente o replay. Limite de resultados é total; **últimas N** define o início por partição. Busca informa quantas mensagens examinou e quantos resultados encontrou. Regex segue a sintaxe do crate Rust `regex`, sem backreferences ou lookaround.

Os consumidores de inspeção usam atribuição explícita de partições, `read_committed` e commit automático desabilitado. Consulta e leitura não avançam os offsets dos grupos existentes. Mensagens de transações ainda abertas podem provocar timeout antes do limite superior do snapshot; a operação informa que o resultado é parcial.

Tarefas podem ser canceladas. Exportações interrompidas preservam um arquivo `.partial`; falhas de importação/replay informam quantos envios já foram confirmados. A aplicação não repete automaticamente uma tarefa com resultado incerto. Os produtores têm idempotência habilitada dentro da sessão; reiniciar um import/replay pode publicar novamente as mesmas mensagens.

Importação e replay mantêm até 8192 envios ou 8 MiB pendentes por operação. Uma mensagem maior usa a janela sozinha. O cancelamento para de enfileirar e aguarda os resultados de todos os envios pendentes antes de retornar. A ordem por partição, bytes, headers e timestamps são preservados conforme as opções da transferência.

A tabela aceita até 20.000 resultados e retém até 128 MiB de dados por leitura/busca. A exportação escreve em fluxo para atender conjuntos maiores.

O aplicativo reutiliza um consumidor de leitura por cluster selecionado e um worker permanente. Cada operação consulta novamente metadados e offsets; ao terminar ou cancelar, remove a atribuição anterior. Mudanças na conexão e nas credenciais recriam o cliente; **Conectar / atualizar todos** também força sua renovação. A primeira mensagem já chega formatada com os resultados. O cliente HTTP/TLS do Schema Registry só é criado quando necessário para Avro registrado.

## Compilar no Windows

Requisitos: Rust stable, Visual Studio C++ Build Tools, CMake, Strawberry Perl e NASM. As bibliotecas Kafka/TLS são compiladas e vinculadas ao aplicativo.

```powershell
.\scripts\build-windows.ps1 -Command build
.\target\debug\kafunekafka.exe
```

O script procura ferramentas instaladas no PATH e as cópias de desenvolvimento em `.tools`. Para otimização de produção:

```powershell
.\scripts\build-windows.ps1 -Command build -Release
```

## Compilar no Linux

Em Ubuntu/Debian:

```bash
sudo apt-get install build-essential pkg-config cmake libssl-dev \
  libxcb-render0-dev libxcb-shape0-dev libxcb-xfixes0-dev \
  libxkbcommon-dev libxkbcommon-x11-dev libgtk-3-dev
cargo build --locked
./target/debug/kafunekafka
```

O renderer nativo usa OpenGL. Funciona com X11 ou Wayland. `cargo build --release --locked` gera a versão otimizada.

## Testes reproduzíveis

O `compose.yaml` cria dois clusters Kafka reais e um Schema Registry, com portas limitadas ao loopback. O ambiente é exclusivamente de desenvolvimento; a senha SASL de teste está deliberadamente pública.

```bash
docker compose up -d
cargo test --lib --locked
cargo test --test kafka_end_to_end --locked -- --ignored --nocapture
cargo test --test bulk_transfer --locked -- --ignored --nocapture
cargo test --test read_session --locked -- --ignored --nocapture
```

O teste de integração cria tópicos com nomes únicos e registra evidências em `artifacts/integration-<sistema>.json`. Ele valida os requisitos 1–11 com Kafka real, incluindo bytes nulos/vazios/binários, headers repetidos, preservação de timestamps, Avro com registry, replay entre clusters, reenvio editado, SASL/PLAIN, cancelamento e rejeição de importação inválida antes de publicar. Os tópicos de fixture permanecem disponíveis para inspecionar a interface.

```bash
./target/debug/kafunekafka --smoke-ui artifacts/ui-linux.png
```

O modo de smoke abre a interface, conecta, carrega uma fixture real, formata uma mensagem, salva um screenshot e fecha a janela. Builds, testes e screenshots nas duas plataformas compõem a evidência do requisito 12.

## Organização

- `model.rs`: perfis e representação fiel das mensagens.
- `backend.rs`: Kafka, limites de leitura, envio e transferências.
- `codec.rs`: visualização e codificação Avro.
- `storage.rs`: perfis, arquivo JSONL e gravação atômica.
- `ui.rs`: interface nativa e tarefas em segundo plano.
- `tests/kafka_end_to_end.rs`: validação com Kafka real.

As funções de criar tópicos no backend atendem ao ambiente de teste; não existem ações destrutivas de gerenciamento na interface inicial.

## Performance

A versão 0.2 usa builds `release`, transferências em lote com memória limitada, exportação com buffer de 256 KiB, busca sem copiar mensagens rejeitadas e lista lateral virtualizada. A fila do consumidor tem limite configurado de 16 MiB; isso limita o prefetch e se soma às mensagens retidas pela tabela e às alocações do cliente. Mensagens individuais e respostas de fetch grandes podem ultrapassar esse tamanho.

`scripts/benchmark.py` mede tempo, CPU e memória do processo em execuções repetidas e preserva as saídas. `scripts/compare_offset_linux.py` funciona em Windows e Linux e compara o CLI oficial do Offset Explorer com o backend usado pelo desktop. Os resultados completos, o conjunto de dados e as limitações ficam em `PERFORMANCE.md`.

O teste das interfaces reais no Windows está em `PERFORMANCE-WINDOWS-GUI.md`. `cargo build --release --bin kafunekafka-ui-bench` e `scripts/benchmark_native_ui.py --same-window` exercitam a UI, o worker, a tabela e a formatação da mensagem selecionada. `scripts/benchmark_desktop.py --uss` separa memória exclusiva residente, residente total e privada comprometida. Os relógios dos dois aplicativos têm fronteiras diferentes.

`scripts/compare_native_ui.py --baseline <runner anterior> --binary target/release/kafunekafka-ui-bench.exe` alterna sessões das duas versões e conserva todas as leituras, inclusive os valores mais lentos. `KAFUNEKAFKA_TRACE=1` habilita tempos por etapa, sem registrar credenciais ou conteúdo das mensagens.

O binário `kafunekafka-bench` é uma ferramenta de desenvolvimento. Antes de executar `fixture`, escolha nomes novos: repetir a geração no mesmo tópico acrescenta mensagens. Os benchmarks não apagam tópicos. A medição deve ocorrer depois dos builds, com os mesmos dados, limites e opções nos dois programas.

## Gerar e publicar o compilado Windows

Após compilar com `cargo build --release --locked --bin kafunekafka`, execute:

```powershell
python scripts/package_windows.py
```

O pacote portátil e o checksum são gerados em `dist/`. O script localiza o runtime x64 do Visual Studio; se necessário, informe seu caminho com `--runtime-dll <vcruntime140.dll>`. Ele verifica os arquivos do ZIP e reúne as licenças a partir de `cargo metadata --locked`.

O workflow `.github/workflows/windows-release.yml` compila no Windows, executa formatação, clippy e testes unitários e prepara o ZIP. Para publicar a versão, atualize `Cargo.toml`, `Cargo.lock` e `RELEASE-NOTES.md` e envie a tag correspondente:

```powershell
git tag v0.3.0
git push origin v0.3.0
```

O workflow confere a tag contra a versão do Cargo antes de anexar o ZIP e o SHA-256 à release. Também pode ser executado manualmente em **Actions → Windows release** para gerar somente os artefatos. Os testes com Kafka real são executados separadamente com o ambiente Docker descrito acima.

## Referências

[Documentação de importação do Offset Explorer](https://www.kafkatool.com/documentation/importtool.html), [documentação de exportação](https://www.kafkatool.com/documentation/exporttool.html), [rust-rdkafka](https://docs.rs/rdkafka/0.39.0/rdkafka/) e [eframe](https://docs.rs/eframe/0.33.3/eframe/). O formato de arquivo deste programa é próprio; não se anuncia compatibilidade com os diretórios binários do Offset Explorer.
