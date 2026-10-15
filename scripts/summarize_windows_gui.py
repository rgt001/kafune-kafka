"""Record the Windows GUI measurements and the human-reported OE timing."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "artifacts/performance"


def main():
    def load(name):
        return json.loads((DATA / (name + ".json")).read_text())
    timing = load("native-ui-windows-lens-same-window")
    previous = load("gui-windows-comparison") if (DATA / "gui-windows-comparison.json").exists() else {}
    memory = {"kafkalens": load("desktop-windows-lens-final-0"),
              "offset_explorer": load("desktop-windows-offset-final-1")}
    result = {"platform": "windows", "date": "2026-10-14", "count": 12000,
              "topic": "kafkalens-perf-balanced-20261014-source",
              "kafkalens_timing": timing["summary"],
              "offset_explorer_timing": {"mean_seconds_reported_by_user": .180,
                                         "requested_repetitions": 5, "individual_samples": None,
                                         "source": "User read the GUI status bar and reported media de 180ms after the five-read request."},
              "memory": {product: {k: row[k] for k in ["pid", "executable", "duration_seconds", "median_rss_bytes_sampled", "median_uss_bytes_sampled", "median_private_bytes_sampled", "cpu_seconds_sampled"]} for product, row in memory.items()},
              "method": "Two real Windows desktop processes, same topic and 12000 retained records, user-operated loading verified by supplied screenshots. Memory sampled at 100ms in an idle 15s interval per process after reloads. Native KafkaLens test runner performs five reads in one window after discarded warmup using the production UI/worker.",
              "limits": "OE timer boundaries are defined by that application; human reported a mean without individual samples. KafkaLens timer includes dispatch, read, selected JSON formatting and table layout. No matched click-to-present latency benchmark. Process histories, foreground/background state and viewport sizes differ. USS/RSS depend on OS paging; Java garbage collection affects retained memory. No GPU memory measurement.",
              "measured_version": "0.2.0",
              "production_windows_binary_sha256": previous.get("production_windows_binary_sha256") or hashlib.sha256((ROOT / "target/release/kafka-lens.exe").read_bytes()).hexdigest(),
              "test_runner_sha256": timing["binary_sha256"]}
    (DATA / "gui-windows-comparison.json").write_text(json.dumps(result, indent=2))
    rows = []
    for label, field in [("RAM exclusiva residente (USS)", "median_uss_bytes_sampled"),
                         ("RAM residente total (RSS)", "median_rss_bytes_sampled"),
                         ("Memória privada comprometida", "median_private_bytes_sampled")]:
        values = [result["memory"][p][field] / 2**20 for p in ["kafkalens", "offset_explorer"]]
        rows.append(f"| {label} | {values[0]:.1f} MiB | {values[1]:.1f} MiB |")
    data_ready = timing["summary"]["data_ready"]
    report = f'''# Interfaces no Windows — KafkaLens e Offset Explorer

Teste de 14/10/2026 com 12.000 mensagens do mesmo tópico local e três partições.

## Memória com as mensagens carregadas

| Medida | KafkaLens 0.2.0 | Offset Explorer 4.0.5 |
|---|---:|---:|
{chr(10).join(rows)}

Valores são medianas das amostras de um intervalo de aproximadamente 15 segundos em repouso por processo. CPU observada nesse intervalo: 0,000 s em ambos, dentro da resolução dos contadores. O número zero não comprova ausência absoluta de atividade.

USS mede memória residente exclusiva; RSS inclui também páginas compartilhadas e módulos mapeados. Memória privada comprometida inclui páginas que podem estar fora da RAM. As medidas têm significados diferentes, conforme a [documentação do psutil](https://psutil.io/api/#psutil.Process.memory_full_info). Foram usados psutil 7.2.2 e processos reais do Windows. Não se mediu VRAM.

Depois das releituras, o KafkaLens ficou abaixo do Offset Explorer nas três medidas. Este é um recorte do estado observado, com históricos diferentes de uso e de coleta de lixo; não é um limite máximo garantido nem uma comparação entre processos recém-abertos. Em uma etapa anterior, o total residente do KafkaLens era maior que o do Offset Explorer. O mapa de memória do KafkaLens mostrou aproximadamente 141 MiB em dois módulos NVIDIA (`nvgpucomp64.dll` e `nvoglv64.dll`), além das demais alocações. Esse custo também faz parte da interface.

## Tempo de carregar os dados

- **KafkaLens, cinco leituras na mesma janela:** mediana **{data_ready['median']*1000:.1f} ms**, média **{data_ready['mean']*1000:.1f} ms**, intervalo **{data_ready['min']*1000:.1f}–{data_ready['max']*1000:.1f} ms**. Houve uma leitura de aproximadamente 907 ms; ela foi mantida no cálculo.
- **Offset Explorer:** média de **180 ms informada pelo usuário**, lendo o indicador da interface após a solicitação de cinco releituras. Os cinco valores individuais não foram registrados.

O nosso relógio vai do disparo da leitura ao recebimento dos registros, formatação JSON da mensagem selecionada e montagem do layout da tabela. Exclui a conexão inicial e a inicialização do processo. O segundo relógio do teste captura o framebuffer renderizado e inclui o custo de readback do screenshot. O relógio do Offset Explorer tem fronteiras definidas pelo próprio programa. Portanto, os números não constituem uma medição idêntica de clique até a apresentação na tela.

**Este teste não comprova superioridade de latência do KafkaLens na GUI Windows.** As observações favorecem o consumo de memória do KafkaLens no estado testado. Os ganhos de 14–17 vezes medidos nos CLIs não se aplicam à interface já aberta.

## Evidências e reprodução

As duas capturas enviadas pelo usuário confirmam 12.000 mensagens no mesmo tópico. A captura inicial do Offset Explorer mostra bytes em hexadecimal e 310 ms; foi solicitado mudar Key/Value para String e repetir antes da média de 180 ms informada posteriormente. As imagens ficam em `artifacts/performance/ui-windows-*-12000-user.png`.

`gui-windows-comparison.json` conserva a comparação; `desktop-windows-*-final-*.json` conserva cada amostra de memória; `native-ui-windows-lens-same-window.json` conserva as leituras e os limites dos relógios. Amostras anteriores foram preservadas e não foram usadas na tabela acima.

```powershell
cargo build --release --locked --bin kafkalens-ui-bench
python scripts/benchmark_native_ui.py --binary target/release/kafkalens-ui-bench.exe --same-window
python scripts/benchmark_desktop.py --pid <PID> --seconds 15 --uss --label <estado>
python scripts/summarize_windows_gui.py
```

O runner usa a UI e o worker reais, com instrumentação restrita ao modo de smoke. A interface normal não mudou. Sete testes unitários e clippy com `-D warnings` passaram após a instrumentação. A comparação externa por captura de janela falhou (`FrameArrived timed out` e `window capture timed out`); o carregamento do Offset Explorer foi feito manualmente pelo usuário. O controle automático foi interrompido pelo Escape e não houve novas ações de interface depois disso.
'''
    if (DATA / "gui-windows-optimization.json").exists():
        optimization = load("gui-windows-optimization")
        result["optimization_0_2_1"] = optimization
        (DATA / "gui-windows-comparison.json").write_text(json.dumps(result, indent=2))
        old = optimization["summary"]["baseline"]["data_ready"]
        new = optimization["summary"]["optimized"]["data_ready"]
        frame = optimization["summary"]["optimized"]["frame_captured"]
        report += f'''
## Otimização 0.2.1 — comparação entre versões

Foram alternadas cinco sessões da versão anterior e cinco da nova, cada uma com uma leitura de aquecimento descartada e nove leituras medidas. São **{new['count']} leituras de 12.000 registros por versão**, na mesma interface nativa, incluindo JSON formatado e tabela. Nenhum valor lento foi removido; o log detalhado de etapas foi desabilitado durante a comparação.

| Tempo até dados e layout prontos | 0.2.0 | 0.2.1 |
|---|---:|---:|
| Mediana | {old['median']*1000:.1f} ms | {new['median']*1000:.1f} ms |
| Média | {old['mean']*1000:.1f} ms | {new['mean']*1000:.1f} ms |
| Mínimo | {old['min']*1000:.1f} ms | {new['min']*1000:.1f} ms |
| Máximo | {old['max']*1000:.1f} ms | {new['max']*1000:.1f} ms |

A mediana caiu {100*(1-new['median']/old['median']):.1f}% ({old['median']/new['median']:.2f} vezes). Incluindo captura do framebuffer, a nova mediana foi {frame['median']*1000:.1f} ms, com intervalo {frame['min']*1000:.1f}–{frame['max']*1000:.1f} ms. Essa captura inclui seu próprio custo e não mede o instante físico em que o monitor exibiu o quadro.

O rastreamento anterior à correção encontrou 100–115 ms para destruir o consumidor após recolher os registros. Uma sessão intermediária capturou 760 ms entre o envio da tarefa e o início da nova thread. Isso localiza a demora, mas não identifica qual componente do Windows provocou a espera. A versão 0.2.1 mantém um consumidor e um worker, remove a atribuição ao terminar/cancelar, consulta novamente metadados e watermarks, reduz `fetch.wait.max.ms` de 100 para 10 ms e formata a primeira mensagem no próprio worker de leitura. JSON/XML e Avro local dispensam inicialização de HTTP/TLS; o cliente do registry é criado apenas quando usado.

**A nova média de {new['mean']*1000:.1f} ms ficou abaixo dos 180 ms informados para o Offset Explorer no teste manual anterior.** A redução entre nossas versões foi medida com o mesmo relógio; a comparação com o Offset Explorer continua tendo relógios e sessões diferentes. Não se repetiu a medição de RAM em repouso do Offset Explorer, e a tabela de memória acima pertence à versão 0.2.0. Os picos de recursos dos dez processos novos estão no JSON, com as amostras individuais.

Nove testes unitários e clippy passaram no Windows e no Linux. A validação real cobre os 12 requisitos, importação/replay em lote e regressões de conexão reutilizada: limites atualizados após novos envios, cancelamento seguido de outro intervalo, partições/offsets diferentes, recuperação de erro e troca de cluster. O Kafka local apresentou timeouts de criação de tópicos durante a validação inicial; os logs foram preservados, os dois containers foram reiniciados sem recriação e os testes foram repetidos.

Evidências: `artifacts/performance/gui-windows-optimization.json`, `gui-ab-*-same-window.json`, `raw/gui-*.stdout` e `raw/gui-*.stderr`. Reprodução:

```powershell
python scripts/compare_native_ui.py --baseline <runner-0.2.0> --binary target/release/kafkalens-ui-bench.exe
```
'''
    (ROOT / "PERFORMANCE-WINDOWS-GUI.md").write_text(report, encoding="utf-8")
    print(json.dumps({"memory": result["memory"], "latency": data_ready}, indent=2))


if __name__ == "__main__":
    main()
