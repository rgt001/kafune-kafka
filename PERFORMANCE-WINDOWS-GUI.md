# Interfaces no Windows — kafunekafka e Offset Explorer

Relatório histórico anterior à renomeação pública para `kafunekafka`. Os nomes de apresentação foram atualizados; os resultados e hashes foram preservados. Estes números não são uma nova medição da versão 0.3.0.

Os caminhos de evidências citados abaixo se referem ao ambiente original de medição. Esses arquivos locais não acompanham esta release pública.

Teste de 14/10/2026 com 12.000 mensagens do mesmo tópico local e três partições.

## Memória com as mensagens carregadas

| Medida | kafunekafka 0.2.0 | Offset Explorer 4.0.5 |
|---|---:|---:|
| RAM exclusiva residente (USS) | 46.9 MiB | 416.6 MiB |
| RAM residente total (RSS) | 291.6 MiB | 694.1 MiB |
| Memória privada comprometida | 115.9 MiB | 628.9 MiB |

Valores são medianas das amostras de um intervalo de aproximadamente 15 segundos em repouso por processo. CPU observada nesse intervalo: 0,000 s em ambos, dentro da resolução dos contadores. O número zero não comprova ausência absoluta de atividade.

USS mede memória residente exclusiva; RSS inclui também páginas compartilhadas e módulos mapeados. Memória privada comprometida inclui páginas que podem estar fora da RAM. As medidas têm significados diferentes, conforme a [documentação do psutil](https://psutil.io/api/#psutil.Process.memory_full_info). Foram usados psutil 7.2.2 e processos reais do Windows. Não se mediu VRAM.

Depois das releituras, o kafunekafka ficou abaixo do Offset Explorer nas três medidas. Este é um recorte do estado observado, com históricos diferentes de uso e de coleta de lixo; não é um limite máximo garantido nem uma comparação entre processos recém-abertos. Em uma etapa anterior, o total residente do kafunekafka era maior que o do Offset Explorer. O mapa de memória do kafunekafka mostrou aproximadamente 141 MiB em dois módulos NVIDIA (`nvgpucomp64.dll` e `nvoglv64.dll`), além das demais alocações. Esse custo também faz parte da interface.

## Tempo de carregar os dados

- **kafunekafka, cinco leituras na mesma janela:** mediana **201.8 ms**, média **340.8 ms**, intervalo **185.7–906.6 ms**. Houve uma leitura de aproximadamente 907 ms; ela foi mantida no cálculo.
- **Offset Explorer:** média de **180 ms informada pelo usuário**, lendo o indicador da interface após a solicitação de cinco releituras. Os cinco valores individuais não foram registrados.

O nosso relógio vai do disparo da leitura ao recebimento dos registros, formatação JSON da mensagem selecionada e montagem do layout da tabela. Exclui a conexão inicial e a inicialização do processo. O segundo relógio do teste captura o framebuffer renderizado e inclui o custo de readback do screenshot. O relógio do Offset Explorer tem fronteiras definidas pelo próprio programa. Portanto, os números não constituem uma medição idêntica de clique até a apresentação na tela.

**Este teste não comprova superioridade de latência do kafunekafka na GUI Windows.** As observações favorecem o consumo de memória do kafunekafka no estado testado. Os ganhos de 14–17 vezes medidos nos CLIs não se aplicam à interface já aberta.

## Evidências e reprodução

As duas capturas enviadas pelo usuário confirmam 12.000 mensagens no mesmo tópico. A captura inicial do Offset Explorer mostra bytes em hexadecimal e 310 ms; foi solicitado mudar Key/Value para String e repetir antes da média de 180 ms informada posteriormente. As imagens ficam em `artifacts/performance/ui-windows-*-12000-user.png`.

`gui-windows-comparison.json` conserva a comparação; `desktop-windows-*-final-*.json` conserva cada amostra de memória; `native-ui-windows-lens-same-window.json` conserva as leituras e os limites dos relógios. Amostras anteriores foram preservadas e não foram usadas na tabela acima.

```powershell
cargo build --release --locked --bin kafunekafka-ui-bench
python scripts/benchmark_native_ui.py --binary target/release/kafunekafka-ui-bench.exe --same-window
python scripts/benchmark_desktop.py --pid <PID> --seconds 15 --uss --label <estado>
python scripts/summarize_windows_gui.py
```

O runner usa a UI e o worker reais, com instrumentação restrita ao modo de smoke. A interface normal não mudou. Sete testes unitários e clippy com `-D warnings` passaram após a instrumentação. A comparação externa por captura de janela falhou (`FrameArrived timed out` e `window capture timed out`); o carregamento do Offset Explorer foi feito manualmente pelo usuário. O controle automático foi interrompido pelo Escape e não houve novas ações de interface depois disso.

## Otimização 0.2.1 — comparação entre versões

Foram alternadas cinco sessões da versão anterior e cinco da nova, cada uma com uma leitura de aquecimento descartada e nove leituras medidas. São **45 leituras de 12.000 registros por versão**, na mesma interface nativa, incluindo JSON formatado e tabela. Nenhum valor lento foi removido; o log detalhado de etapas foi desabilitado durante a comparação.

| Tempo até dados e layout prontos | 0.2.0 | 0.2.1 |
|---|---:|---:|
| Mediana | 185.7 ms | 62.8 ms |
| Média | 237.1 ms | 62.8 ms |
| Mínimo | 171.1 ms | 57.2 ms |
| Máximo | 1650.4 ms | 71.5 ms |

A mediana caiu 66.2% (2.96 vezes). Incluindo captura do framebuffer, a nova mediana foi 70.5 ms, com intervalo 63.9–145.4 ms. Essa captura inclui seu próprio custo e não mede o instante físico em que o monitor exibiu o quadro.

O rastreamento anterior à correção encontrou 100–115 ms para destruir o consumidor após recolher os registros. Uma sessão intermediária capturou 760 ms entre o envio da tarefa e o início da nova thread. Isso localiza a demora, mas não identifica qual componente do Windows provocou a espera. A versão 0.2.1 mantém um consumidor e um worker, remove a atribuição ao terminar/cancelar, consulta novamente metadados e watermarks, reduz `fetch.wait.max.ms` de 100 para 10 ms e formata a primeira mensagem no próprio worker de leitura. JSON/XML e Avro local dispensam inicialização de HTTP/TLS; o cliente do registry é criado apenas quando usado.

**A nova média de 62.8 ms ficou abaixo dos 180 ms informados para o Offset Explorer no teste manual anterior.** A redução entre nossas versões foi medida com o mesmo relógio; a comparação com o Offset Explorer continua tendo relógios e sessões diferentes. Não se repetiu a medição de RAM em repouso do Offset Explorer, e a tabela de memória acima pertence à versão 0.2.0. Os picos de recursos dos dez processos novos estão no JSON, com as amostras individuais.

Nove testes unitários e clippy passaram no Windows e no Linux. A validação real cobre os 12 requisitos, importação/replay em lote e regressões de conexão reutilizada: limites atualizados após novos envios, cancelamento seguido de outro intervalo, partições/offsets diferentes, recuperação de erro e troca de cluster. O Kafka local apresentou timeouts de criação de tópicos durante a validação inicial; os logs foram preservados, os dois containers foram reiniciados sem recriação e os testes foram repetidos.

Evidências: `artifacts/performance/gui-windows-optimization.json`, `gui-ab-*-same-window.json`, `raw/gui-*.stdout` e `raw/gui-*.stderr`. Reprodução:

```powershell
python scripts/compare_native_ui.py --baseline <runner-0.2.0> --binary target/release/kafunekafka-ui-bench.exe
```
