# kafunekafka 0.2.0 — medições de performance

Relatório histórico anterior à renomeação pública para `kafunekafka`. Os nomes de apresentação foram atualizados; os resultados e hashes foram preservados. Estes números não são uma nova medição da versão 0.3.0.

Os caminhos de evidências citados abaixo se referem ao ambiente original de medição. Esses arquivos locais não acompanham esta release pública.

Medições locais de 14/10/2026, contra o Offset Explorer 4.0.5. A versão otimizada apresenta vantagem em tempo e memória nos CLIs testados. A inicialização da JVM faz parte desses tempos; a vantagem não pode ser aplicada diretamente à interface já aberta.

## Ambiente e método

AMD Ryzen 7 5700G, 8 cores/16 threads, 32 GB de RAM; Windows 10 Pro 19045 e Ubuntu 22.04 no WSL2. Kafka 4.1.0 e Schema Registry 7.8.3 locais. kafunekafka compilado em `release`, LTO thin; Offset Explorer instalado pelos distribuidores oficiais, com Java 21. Compilações e testes de integração terminaram antes das medições finais.

Cada comparação de CLI usa uma execução de aquecimento descartada e cinco execuções medidas por produto, alternando a ordem. A tabela mostra a mediana e o intervalo mínimo–máximo em segundos. Os dois programas leem as mesmas 12.000 mensagens JSON (~940 bytes de valor), com chave, timestamp fixo e três partições de 4.000 mensagens. A busca regex retorna as mesmas 120 mensagens; o SHA-256 dos registros normalizados comprova igualdade do conteúdo em cada par de leitura/busca.

`psutil` amostra processo e filhos a cada 20 ms. Picos de RSS e CPU são estimativas por amostragem e podem omitir eventos entre amostras; CPU é tempo agregado das threads, não percentual. RSS de processos com páginas compartilhadas não equivale a consumo físico exclusivo. No Linux, stdout e arquivos JSONL ficam em `/mnt/d` (DrvFS); esse custo de filesystem participa do tempo total. O tempo completo inclui inicialização, conexão, processamento, saída e encerramento.

## CLIs — leitura e busca

| Plataforma / operação | kafunekafka s (min–max) | Offset Explorer s (min–max) | Razão OE/kafunekafka | RSS kafunekafka / OE MiB | CPU kafunekafka / OE s |
|---|---:|---:|---:|---:|---:|
| windows / leitura + stdout | 0.221 (0.199–0.374) | 3.717 (3.636–3.937) | 16.8× | 15.0 / 196.4 | 0.078 / 5.047 |
| windows / busca regex + stdout | 0.260 (0.199–0.322) | 3.650 (3.629–3.880) | 14.1× | 14.3 / 193.8 | 0.062 / 4.812 |
| linux / leitura + stdout | 1.369 (1.206–1.393) | 7.771 (7.237–8.439) | 5.7× | 26.0 / 199.2 | 0.170 / 5.480 |
| linux / busca regex + stdout | 0.209 (0.173–0.210) | 3.753 (3.693–3.822) | 17.9× | 17.5 / 203.9 | 0.040 / 4.590 |

O kafunekafka usa o mesmo `Backend` do desktop por meio de um driver de desenvolvimento. O outro lado usa o [CLI oficial do Offset Explorer](https://www.kafkatool.com/documentation/commandline.html). Isso compara duas execuções completas de CLI, incluindo seus runtimes. As condições são Kafka local, plaintext e JSON; TLS, Avro, latência WAN e clusters de produção não foram comparados em performance.

## CLIs — replay confirmado de 12.000 mensagens

| Plataforma | kafunekafka completo s (min–max) | Offset Explorer completo s (min–max) | RSS kafunekafka / OE MiB | Tempo informado kafunekafka / OE s |
|---|---:|---:|---:|---:|
| windows | 0.405 (0.362–0.511) | 2.754 (2.587–2.877) | 25.7 / 253.2 | 0.367 / 0.564 |
| linux | 0.311 (0.310–0.351) | 2.450 (2.368–2.801) | 25.8 / 234.3 | 0.295 / 0.403 |

Os relógios informados pelos produtos têm fronteiras diferentes: o nosso inclui validação do destino e configuração do scan; o do Offset Explorer é definido por ele. São valores de diagnóstico. Em uma execução adicional com o Offset Explorer já aberto no X11, a interface informou **306 ms** para as mesmas 12.000 mensagens. O nosso backend no Linux ficou perto de 295 ms, mas esses números não estabelecem um vencedor de latência da GUI. A captura está em `.tools/evidence/offset-warm-replay-0.png`.

## Operações do kafunekafka

Cinco execuções por operação, aquecimento descartado, 10.000 mensagens na fixture original. Exportação usa JSONL com bytes em base64 e `fsync`; importação valida todo o arquivo antes de publicar e espera confirmações. O formato do Offset Explorer exporta arquivos individuais e não é equivalente ao nosso JSONL; não usamos esses formatos para anunciar uma vantagem direta de exportação/importação.

| Plataforma / operação | Tempo completo s (min–max) | RSS MiB |
|---|---:|---:|
| windows / snapshot | 0.199 (0.197–0.201) | 8.7 |
| windows / read | 0.200 (0.197–0.203) | 21.3 |
| windows / search | 0.200 (0.199–0.223) | 14.2 |
| windows / export | 0.241 (0.219–0.268) | 14.6 |
| windows / import | 0.767 (0.743–0.810) | 19.0 |
| linux / snapshot | 0.168 (0.168–0.169) | 10.5 |
| linux / read | 0.148 (0.148–0.189) | 25.6 |
| linux / search | 0.149 (0.147–0.188) | 20.7 |
| linux / export | 0.454 (0.330–0.962) | 23.1 |
| linux / import | 0.372 (0.371–0.426) | 19.5 |

A importação original confirmava uma mensagem por vez: os dois registros de 10.000 mensagens (aquecimento e primeira medição) têm mediana de tempo interno de **155.14 s**. A versão final tem mediana interna de **0.741 s**, em cinco medições. O número de repetições é diferente; não se trata de comparação controlada entre versões. O harness antigo também escrevia os 1.000 recibos em stderr em pequenas chamadas; esse custo foi removido do driver e não é apresentado como otimização do produto.

## Desktop e limites de memória

- `desktop-linux-lens-loaded`: pico RSS amostrado **134.3 MiB**, CPU **0.000 s** durante 10.0 s em repouso após carregar 12.000 mensagens.
- `desktop-linux-offset-loaded`: pico RSS amostrado **358.2 MiB**, CPU **0.040 s** durante 10.0 s em repouso após carregar 12.000 mensagens.

A medição das interfaces Linux é um recorte em Xvfb/OpenGL por software, com históricos de execução diferentes; não é um benchmark de scrolling ou p95/p99 e não mede a GPU. A captura externa de janelas no Windows falhou; foi realizado depois um teste das interfaces reais com carregamento manual pelo usuário e instrumentação da nossa UI. Os dados de memória e os limites da comparação de tempos estão em [PERFORMANCE-WINDOWS-GUI.md](PERFORMANCE-WINDOWS-GUI.md). O smoke do aplicativo é executado nas duas plataformas.

Transferências limitam a janela pendente a **8 MiB estimados ou 8.192 mensagens**, incluindo estimativa de overhead. Uma mensagem maior ocupa a janela sozinha. Esse orçamento não limita o RSS total: librdkafka, buffers de rede, decodificação e GUI alocam memória adicional. Prefetch configurado em 16 MiB também pode ser excedido por uma mensagem/fetch grande. Leitura/busca retém até 128 MiB estimados de registros; a tabela permite até 20.000 resultados. Exportação e replay são streaming.

## Stress de 100.000 mensagens no Windows

Fixture adicional (~94 MB de valores), uma execução de aquecimento e três medidas. Estes testes avaliam o streaming do kafunekafka; o Offset Explorer não foi comparado neste volume.

| Operação | Tempo completo s (min–max) | RSS MiB | CPU agregada s |
|---|---:|---:|---:|
| export | 0.999 (0.877–1.076) | 15.4 | 0.656 |
| replay | 6.349 (4.345–7.264) | 39.8 | 6.500 |

Cada exportação de 100.000 registros foi conferida contra a fixture por hash por partição, preservando ordem, offsets, chave, valor, headers e timestamp. Todos os replays informaram 100.000 confirmações. O teste de integração separado confere também conteúdo e ordem no destino. O tempo de replay neste volume ainda tem variação relevante; este recorte não estabelece um limite máximo de throughput nem uma vantagem sobre o concorrente em volumes grandes.

## Alterações e validação

- Producer de lote com callbacks no worker, backpressure e drenagem das confirmações em sucesso, cancelamento e falha. Idempotência e `acks=all` continuam habilitados.
- Arquivo atômico com buffer de 256 KiB; leitor reutiliza linha e rejeita linhas acima de 64 MiB.
- Busca copia apenas registros aceitos, preservando a formatação JSON/XML/Avro usada na busca.
- Interface evita clones completos por frame e virtualiza a lista de tópicos; consumidor usa prefetch limitado e commits automáticos desabilitados.
- Sete testes unitários por plataforma; integração Kafka dos 12 requisitos; teste adicional com 2.050 mensagens de ~4 KiB, três partições, tombstones, headers binários/repetidos, timestamps, ordem, cancelamento e falha de enqueue; clippy com `-D warnings` nas duas plataformas.

Durante uma compilação concorrente anterior, uma fixture Linux excedeu o prazo de metadados de 10 s; a repetição passou. A versão final passou novamente nos testes completos das duas plataformas. Logs e execuções de diagnóstico permanecem em `.tools/evidence` e `artifacts/performance/raw`; tabelas acima usam apenas os conjuntos finais nomeados.

## Reprodução

Instale `psutil` em `.tools/perf-python` no Windows (`python -m pip install --target .tools/perf-python psutil`) ou no ambiente Python do Linux. Compile `cargo build --release --locked` e suba `docker compose up -d`. Instale o Offset Explorer separadamente pelo [site oficial](https://www.kafkatool.com/download.html), respeitando a licença; ele não é redistribuído nos pacotes.

Crie uma conexão `kafunekafka-perf` no CLI oficial e gere a fixture **uma única vez em tópicos novos**. Repetir `fixture` no mesmo tópico acrescenta mensagens. Ajuste os caminhos dos binários nos scripts se sua instalação for diferente.

```text
commandline -addServerConnection -name kafunekafka-perf -bootstrapServers localhost:19092 -version 4.1 -security plaintext -testConnection true
kafunekafka-bench fixture kafunekafka-perf-balanced-20261014-source 12000 fixture.jsonl kafunekafka-perf-balanced-20261014-destination 3
python scripts/compare_offset_linux.py --trials 5
python scripts/compare_replay.py --trials 5 --offset-project benchmarks/offset-replay.json
python scripts/benchmark.py --binary <kafunekafka-bench> --label <nome> --trials 5
python scripts/benchmark_desktop.py --pid <pid> --seconds 10 --label <estado-da-interface>
python scripts/render_performance_report.py --linux-binary <kafunekafka-bench-linux>
```

O driver geral usa uma fixture de 10.000 mensagens e `.tools/perf-input.jsonl` para importação: gere essa fixture separadamente ou passe `--topic`, `--destination` e `--count` adequados. Os arquivos de resumo JSON e as saídas brutas permanecem no ambiente original de medição.

## Identificação dos binários medidos

| Arquivo | SHA-256 |
|---|---|
| kafunekafka-bench.exe (Windows) | `1a44fa53c817e6c581a5faece38d3a663d8a2a6499726306cf9b8d3fa8d2f0a6` |
| kafunekafka-bench (Linux) | `a599d6b1a3e276235f541bc4582cfa91ea314c49da4d3d9a24fed85d2fb8b4a3` |
