"""Render the report from retained, completed benchmark runs."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "artifacts/performance"


def load(name):
    return json.loads((DATA / (name + ".json")).read_text())


def timing(metric):
    return f'{metric["median"]:.3f} ({metric["min"]:.3f}–{metric["max"]:.3f})'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--linux-binary", type=Path, required=True)
    args = parser.parse_args()
    lines = ["# kafunekafka 0.2.0 — medições de performance", "",
             "Medições locais de 14/10/2026, contra o Offset Explorer 4.0.5. A versão otimizada apresenta vantagem em tempo e memória nos CLIs testados. A inicialização da JVM faz parte desses tempos; a vantagem não pode ser aplicada diretamente à interface já aberta.", "",
             "## Ambiente e método", "",
             "AMD Ryzen 7 5700G, 8 cores/16 threads, 32 GB de RAM; Windows 10 Pro 19045 e Ubuntu 22.04 no WSL2. Kafka 4.1.0 e Schema Registry 7.8.3 locais. kafunekafka compilado em `release`, LTO thin; Offset Explorer instalado pelos distribuidores oficiais, com Java 21. Compilações e testes de integração terminaram antes das medições finais.", "",
             "Cada comparação de CLI usa uma execução de aquecimento descartada e cinco execuções medidas por produto, alternando a ordem. A tabela mostra a mediana e o intervalo mínimo–máximo em segundos. Os dois programas leem as mesmas 12.000 mensagens JSON (~940 bytes de valor), com chave, timestamp fixo e três partições de 4.000 mensagens. A busca regex retorna as mesmas 120 mensagens; o SHA-256 dos registros normalizados comprova igualdade do conteúdo em cada par de leitura/busca.", "",
             "`psutil` amostra processo e filhos a cada 20 ms. Picos de RSS e CPU são estimativas por amostragem e podem omitir eventos entre amostras; CPU é tempo agregado das threads, não percentual. RSS de processos com páginas compartilhadas não equivale a consumo físico exclusivo. No Linux, stdout e arquivos JSONL ficam em `/mnt/d` (DrvFS); esse custo de filesystem participa do tempo total. O tempo completo inclui inicialização, conexão, processamento, saída e encerramento.", "",
             "## CLIs — leitura e busca", "",
             "| Plataforma / operação | kafunekafka s (min–max) | Offset Explorer s (min–max) | Razão OE/kafunekafka | RSS kafunekafka / OE MiB | CPU kafunekafka / OE s |",
             "|---|---:|---:|---:|---:|---:|"]
    for platform in ["windows", "linux"]:
        data = load("comparison-" + platform)
        assert data["content_equal"]
        for operation, name in [("view", "leitura + stdout"), ("search-view", "busca regex + stdout")]:
            a, b = (data["summary"][operation][p] for p in ["kafunekafka-0.2.0", "offset-explorer-4.0.5"])
            lines.append(f'| {platform} / {name} | {timing(a["wall_seconds"])} | {timing(b["wall_seconds"])} | {b["wall_seconds"]["median"]/a["wall_seconds"]["median"]:.1f}× | {a["peak_rss_bytes_sampled"]["median"]/2**20:.1f} / {b["peak_rss_bytes_sampled"]["median"]/2**20:.1f} | {a["cpu_seconds_sampled"]["median"]:.3f} / {b["cpu_seconds_sampled"]["median"]:.3f} |')
    lines += ["", "O kafunekafka usa o mesmo `Backend` do desktop por meio de um driver de desenvolvimento. O outro lado usa o [CLI oficial do Offset Explorer](https://www.kafkatool.com/documentation/commandline.html). Isso compara duas execuções completas de CLI, incluindo seus runtimes. As condições são Kafka local, plaintext e JSON; TLS, Avro, latência WAN e clusters de produção não foram comparados em performance.", "",
              "## CLIs — replay confirmado de 12.000 mensagens", "",
              "| Plataforma | kafunekafka completo s (min–max) | Offset Explorer completo s (min–max) | RSS kafunekafka / OE MiB | Tempo informado kafunekafka / OE s |",
              "|---|---:|---:|---:|---:|"]
    for platform in ["windows", "linux"]:
        data = load("replay-comparison-" + platform)
        assert all(r["delivered"] == 12000 for r in data["samples"])
        a, b = (data["summary"][p] for p in ["kafunekafka-0.2.0", "offset-explorer-4.0.5"])
        lines.append(f'| {platform} | {timing(a["wall_seconds"])} | {timing(b["wall_seconds"])} | {a["peak_rss_bytes_sampled"]["median"]/2**20:.1f} / {b["peak_rss_bytes_sampled"]["median"]/2**20:.1f} | {a["operation_seconds_reported"]["median"]:.3f} / {b["operation_seconds_reported"]["median"]:.3f} |')
    lines += ["", "Os relógios informados pelos produtos têm fronteiras diferentes: o nosso inclui validação do destino e configuração do scan; o do Offset Explorer é definido por ele. São valores de diagnóstico. Em uma execução adicional com o Offset Explorer já aberto no X11, a interface informou **306 ms** para as mesmas 12.000 mensagens. O nosso backend no Linux ficou perto de 295 ms, mas esses números não estabelecem um vencedor de latência da GUI. A captura está em `.tools/evidence/offset-warm-replay-0.png`.", "",
              "## Operações do kafunekafka", "",
              "Cinco execuções por operação, aquecimento descartado, 10.000 mensagens na fixture original. Exportação usa JSONL com bytes em base64 e `fsync`; importação valida todo o arquivo antes de publicar e espera confirmações. O formato do Offset Explorer exporta arquivos individuais e não é equivalente ao nosso JSONL; não usamos esses formatos para anunciar uma vantagem direta de exportação/importação.", "",
              "| Plataforma / operação | Tempo completo s (min–max) | RSS MiB |",
              "|---|---:|---:|"]
    for platform in ["windows", "linux"]:
        for operation, metrics in load("release-final-" + platform)["summary"].items():
            lines.append(f'| {platform} / {operation} | {timing(metrics["wall_seconds"])} | {metrics["peak_rss_bytes_sampled"]["median"]/2**20:.1f} |')
    baseline = load("baseline")
    old_import = [s["backend"]["elapsed_seconds"] for s in baseline["samples"] if s["operation"] == "import"]
    current = load("release-final-windows")
    new_import = [s["backend"]["elapsed_seconds"] for s in current["samples"] if s["operation"] == "import" and not s["warmup"]]
    lines += ["", f'A importação original confirmava uma mensagem por vez: os dois registros de 10.000 mensagens (aquecimento e primeira medição) têm mediana de tempo interno de **{statistics.median(old_import):.2f} s**. A versão final tem mediana interna de **{statistics.median(new_import):.3f} s**, em cinco medições. O número de repetições é diferente; não se trata de comparação controlada entre versões. O harness antigo também escrevia os 1.000 recibos em stderr em pequenas chamadas; esse custo foi removido do driver e não é apresentado como otimização do produto.', "",
              "## Desktop e limites de memória", ""]
    for name in ["desktop-linux-lens-loaded", "desktop-linux-offset-loaded"]:
        path = DATA / (name + ".json")
        if path.exists():
            row = load(name)
            lines.append(f'- `{name}`: pico RSS amostrado **{row["peak_rss_bytes_sampled"]/2**20:.1f} MiB**, CPU **{row["cpu_seconds_sampled"]:.3f} s** durante {row["duration_seconds"]:.1f} s em repouso após carregar 12.000 mensagens.')
    windows_note = "A captura externa de janelas no Windows falhou; foi realizado depois um teste das interfaces reais com carregamento manual pelo usuário e instrumentação da nossa UI. Os dados de memória e os limites da comparação de tempos estão em [PERFORMANCE-WINDOWS-GUI.md](PERFORMANCE-WINDOWS-GUI.md)." if (DATA / "gui-windows-comparison.json").exists() else "O acesso à captura nativa do Windows falhou na sessão de automação; a comparação quantitativa das GUIs fica limitada ao Linux."
    lines += ["", "A medição das interfaces Linux é um recorte em Xvfb/OpenGL por software, com históricos de execução diferentes; não é um benchmark de scrolling ou p95/p99 e não mede a GPU. " + windows_note + " O smoke do aplicativo é executado nas duas plataformas.", "",
              "Transferências limitam a janela pendente a **8 MiB estimados ou 8.192 mensagens**, incluindo estimativa de overhead. Uma mensagem maior ocupa a janela sozinha. Esse orçamento não limita o RSS total: librdkafka, buffers de rede, decodificação e GUI alocam memória adicional. Prefetch configurado em 16 MiB também pode ser excedido por uma mensagem/fetch grande. Leitura/busca retém até 128 MiB estimados de registros; a tabela permite até 20.000 resultados. Exportação e replay são streaming.", "",
              "## Stress de 100.000 mensagens no Windows", "",
              "Fixture adicional (~94 MB de valores), uma execução de aquecimento e três medidas. Estes testes avaliam o streaming do kafunekafka; o Offset Explorer não foi comparado neste volume.", "",
              "| Operação | Tempo completo s (min–max) | RSS MiB | CPU agregada s |",
              "|---|---:|---:|---:|"]
    for operation, metrics in load("stress-100k-windows")["summary"].items():
        lines.append(f'| {operation} | {timing(metrics["wall_seconds"])} | {metrics["peak_rss_bytes_sampled"]["median"]/2**20:.1f} | {metrics["cpu_seconds_sampled"]["median"]:.3f} |')
    verification = load("stress-100k-archives")
    assert verification["status"] == "passed"
    lines += ["", "Cada exportação de 100.000 registros foi conferida contra a fixture por hash por partição, preservando ordem, offsets, chave, valor, headers e timestamp. Todos os replays informaram 100.000 confirmações. O teste de integração separado confere também conteúdo e ordem no destino. O tempo de replay neste volume ainda tem variação relevante; este recorte não estabelece um limite máximo de throughput nem uma vantagem sobre o concorrente em volumes grandes.", "",
              "## Alterações e validação", "",
              "- Producer de lote com callbacks no worker, backpressure e drenagem das confirmações em sucesso, cancelamento e falha. Idempotência e `acks=all` continuam habilitados.",
              "- Arquivo atômico com buffer de 256 KiB; leitor reutiliza linha e rejeita linhas acima de 64 MiB.",
              "- Busca copia apenas registros aceitos, preservando a formatação JSON/XML/Avro usada na busca.",
              "- Interface evita clones completos por frame e virtualiza a lista de tópicos; consumidor usa prefetch limitado e commits automáticos desabilitados.",
              "- Sete testes unitários por plataforma; integração Kafka dos 12 requisitos; teste adicional com 2.050 mensagens de ~4 KiB, três partições, tombstones, headers binários/repetidos, timestamps, ordem, cancelamento e falha de enqueue; clippy com `-D warnings` nas duas plataformas.", "",
              "Durante uma compilação concorrente anterior, uma fixture Linux excedeu o prazo de metadados de 10 s; a repetição passou. A versão final passou novamente nos testes completos das duas plataformas. Logs e execuções de diagnóstico permanecem em `.tools/evidence` e `artifacts/performance/raw`; tabelas acima usam apenas os conjuntos finais nomeados.", "",
              "## Reprodução", "",
              "Instale `psutil` em `.tools/perf-python` no Windows (`python -m pip install --target .tools/perf-python psutil`) ou no ambiente Python do Linux. Compile `cargo build --release --locked` e suba `docker compose up -d`. Instale o Offset Explorer separadamente pelo [site oficial](https://www.kafkatool.com/download.html), respeitando a licença; ele não é redistribuído nos pacotes.", "",
              "Crie uma conexão `kafunekafka-perf` no CLI oficial e gere a fixture **uma única vez em tópicos novos**. Repetir `fixture` no mesmo tópico acrescenta mensagens. Ajuste os caminhos dos binários nos scripts se sua instalação for diferente.", "",
              "```text",
              "commandline -addServerConnection -name kafunekafka-perf -bootstrapServers localhost:19092 -version 4.1 -security plaintext -testConnection true",
              "kafunekafka-bench fixture kafunekafka-perf-balanced-20261014-source 12000 fixture.jsonl kafunekafka-perf-balanced-20261014-destination 3",
              "python scripts/compare_offset_linux.py --trials 5",
              "python scripts/compare_replay.py --trials 5 --offset-project benchmarks/offset-replay.json",
              "python scripts/benchmark.py --binary <kafunekafka-bench> --label <nome> --trials 5",
              "python scripts/benchmark_desktop.py --pid <pid> --seconds 10 --label <estado-da-interface>",
              "python scripts/render_performance_report.py --linux-binary <kafunekafka-bench-linux>",
              "```", "",
              "O driver geral usa a fixture original `kafunekafka-perf-20261014-source` de 10.000 mensagens e `.tools/perf-input.jsonl` para importação: gere essa fixture separadamente ou passe `--topic`, `--destination` e `--count` adequados. Os arquivos de resumo JSON acompanham os pacotes; saídas brutas permanecem no checkout local.", "",
              "## Identificação dos binários medidos", "",
              "| Arquivo | SHA-256 |", "|---|---|"]
    paths = [ROOT / "target/release/kafunekafka-bench.exe", args.linux_binary]
    hashes = []
    for path in paths:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f'| {path.name} ({"Windows" if path.suffix == ".exe" else "Linux"}) | `{digest}` |')
        hashes.append({"binary": str(path), "sha256": digest})
    (DATA / "measured-binaries.json").write_text(json.dumps(hashes, indent=2))
    (ROOT / "PERFORMANCE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
