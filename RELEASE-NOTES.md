# KafkaLens 0.2.1

Aplicativo nativo em Rust para inspecionar Kafka no Windows e no Linux.

- Clusters, brokers, tópicos, partições e consumer groups.
- Mensagens e offsets, com JSON, XML e Avro formatados.
- Envio, reenvio com edição, busca por texto ou regex.
- Importação/exportação de arquivos e replay entre tópicos e clusters.
- Conexão e worker reutilizados para reduzir latência nas leituras.

## Windows x64

Baixe `KafkaLens-v0.2.1-windows-x64.zip`, extraia todo o conteúdo e execute `KafkaLens.exe`. Mantenha as DLLs junto do executável. Não é necessário instalar Rust, Java ou o Offset Explorer. Docker é usado apenas no ambiente de desenvolvimento e testes; para usar o aplicativo, basta ter acesso ao seu cluster Kafka.

O arquivo `.sha256` permite conferir o download com `Get-FileHash -Algorithm SHA256`. O ZIP contém as licenças das dependências. Os arquivos de conexão ficam no perfil local do usuário e não acompanham o pacote.

## Validação desta versão

Em 14/10/2026: nove testes unitários, clippy e testes reais com dois clusters Kafka passaram no Windows e no Linux. Os testes cobrem os 12 requisitos e regressões de importação/replay, cancelamento e reutilização da conexão. TLS/SCRAM e clusters de produção não foram exercitados.

Na GUI Windows, 45 leituras de 12.000 mensagens por versão reduziram a mediana de 185,7 ms para 62,8 ms. O relatório `PERFORMANCE-WINDOWS-GUI.md` apresenta os resultados e os limites da comparação com o Offset Explorer.
