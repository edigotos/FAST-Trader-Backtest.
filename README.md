# FAST Trader Backtest

Projeto para baixar candles históricos públicos da Binance e comparar duas estratégias:
1. **EDIGO/EDINOT + SMA21**: entrada após confirmação do cruzamento, saída/reversão no sinal oposto EDIGO/EDINOT.
2. **SMA21 pura**: entrada após cruzamento confirmado e reversão no cruzamento oposto.

## Parâmetros congelados para o primeiro estudo
- Banca inicial: **US$ 1.000**
- Margem por operação: **5% do saldo atualizado**
- Alavancagem: **5x**
- Stop: **70% da margem comprometida por operação** (equivale a movimento adverso aproximado de 14%/5 = 14% no preço, antes de custos, se a posição for 5x; observar que 70% / 5 = 14%)
- Saldo e margem recalculados após cada operação.
- Testar os últimos 12 meses e posteriormente múltiplos pares.
- Não modificar os parâmetros até obter os resultados iniciais.

**Atenção:** sinais EDIGO/EDINOT exigem a fórmula Pine Script exata para reproduzir fielmente a primeira estratégia. Sem isso, não há backtest válido da estratégia 1. O código inicial baixa os dados; não simula resultados inventados.

## Dataset versionado — 2h, futuros USD-M

Snapshot gerado em **2026-11-20 15:21 UTC** (último candle aberto: **2026-11-19 22:00 UTC**, fechado às 23:59:59). Fonte: [data.binance.vision](https://data.binance.vision/) — arquivos mensais + diários para o mês ainda não publicado em formato mensal, com SHA-1/SHA-256 conferido por ZIP.

| Símbolo | Candles | Primeiro candle (UTC) | Último candle (UTC) | Lacunas 2h | Backfill API | Arquivos | MB | Último close | SHA-256 |
|---|---:|---|---|---:|---:|---:|---:|---:|---|
| BTCUSDT | 30.180 | 2020-01-01 00:00 | 2026-11-19 22:00 | 0 | 0 | 101 | 5,17 | 61.959,30 | `0c940e23ec9c` |
| ETHUSDT | 30.180 | 2020-01-01 00:00 | 2026-11-19 22:00 | 0 | 0 | 101 | 5,08 | 1.756,73 | `572eeb3a761b` |
| XRPUSDT | 30.116 | 2020-01-06 08:00 | 2026-11-19 22:00 | 0 | 60 | 101 | 4,92 | 0,9599 | `6c3b3582c379` |
| SOLUSDT | 27.093 | 2020-09-14 06:00 | 2026-11-19 22:00 | 0 | 60 | 93 | 4,41 | 73,11 | `4a2f27f99ef7` |
| ALICEUSDT | 24.899 | 2021-03-16 02:00 | 2026-11-19 22:00 | 0 | 0 | 87 | 3,84 | 0,1038 | `4c5127992141` |
| **Total** | **142.468** | | | **0** | **120** | | **23,4** | | |

Arquivos: `data/futures/2h/<SÍMBOLO>.csv`, a série longa `data/futures/2h/ALL_SYMBOLS.csv` (142.468 linhas ordenadas por `open_time`, depois `symbol`) e o `data/futures/2h/manifest.json` com hashes, contagens e janela de cada par.

**Colunas** (mesma ordem do CSV público da Binance, sem a coluna `ignore`):
`symbol, open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, open_utc, close_utc`

- `open_time` / `close_time` em **milissegundos UTC** (epoch); `open_utc` / `close_utc` são a mesma data em texto legível, para conferência visual e alinhamento com o TradingView.
- Grade **contínua**: cada par cobre todos os slots de 2h entre o primeiro e o último candle, sem buracos. Os 120 candles de XRP/SOL faltantes nos ZIPs mensais de fev–abr/2022 não eram parada de mercado, e sim falha de publicação: foram repostos pelos ZIPs **diários** da própria Vision (`scripts/backfill_gaps.py`) e conferidos contra a API REST — 60/60 linhas idênticas em cada par.
- Janela do estudo de 12 meses: **2025-11-20 00:00 → 2026-11-19 22:00 UTC** = 4.380 candles por par; no recorte exato 18/11/2025 → 18/11/2026 são 4.392. Não há ajuste de fuso no CSV: tudo em UTC.

## Baixar histórico

Python 3.10+, somente biblioteca padrão.

```bash
# os 5 pares do estudo, histórico completo (default: tudo que a Vision publica)
python scripts/download_binance.py --symbols BTCUSDT,ETHUSDT,ALICEUSDT,XRPUSDT,SOLUSDT \
    --interval 2h --market futures --full

# janela de 12 meses, como no workflow do Actions
python scripts/download_binance.py --symbols BTCUSDT,ETHUSDT,ALICEUSDT,XRPUSDT,SOLUSDT \
    --interval 2h --months 12

# fechar buracos de publicacao: default sao os ZIPs diarios da Vision (--source api forza a REST)
python scripts/backfill_gaps.py --dataset-dir data --market futures --interval 2h
```

O script grava os arquivos por mês/dia em `data/raw/` (fora do versionamento) e a série consolidada em `data/futures/2h/`. Mensal + diário são combinados automaticamente: o mês corrente, que ainda não tem ZIP mensal, é montado a partir dos ZIPs diários, então a série vai até ontem sem intervenção manual. Para mercado spot, use `--market spot`; a periodicidade deve ser a mesma usada no TradingView.

### Coleção ampla (todos os pares USDT)

Snapshot **2026-11-20 16:40 UTC**, backfill aplicado às 16:51 UTC:

| Métrica | Valor |
|---|---:|
| Pares USDT com série 2h | 918 |
| Em negociação / delistados ou pausados | 517 / 401 |
| Total de candles | 8.473.312 |
| Janela | 2020-01-01 00:00 → 2026-11-19 22:00 UTC |
| Tamanho descompactado | 1,36 GB |
| Candles repostos dos ZIPs diários | 2.892 (50 pares) |
| Pares com ausência estrutural | 16 |

Listagens por ano: 2020 → 81, 2021 → 59, 2022 → 26, 2023 → 99, 2024 → 131,
2025 → 241, 2026 → 281.

O pacote completo (918 CSVs + `manifest.json`, 464 MB compactados) está no asset da
release [`dataset-2h-all-usdt-2026-11-20`](../../releases/tag/dataset-2h-all-usdt-2026-11-20);
o SHA-256 do arquivo é `9bb582a6d984cadebdf86bb5ba77afa10d8f0b4fd0afd8e8f180c715d1eb8738`.
Um par equivale a um CSV por linha longa (`ALL_SYMBOLS.csv`), então o pacote não é
commitado no histórico do git — apenas o índice `data/futures/2h/manifest_all_usdt.json`
(contagem, janela, último close e SHA-256 de cada par).

Cinco pastas listadas na Vision não têm ZIP mensal de 2h publicado e ficaram de fora:
`哈基米USDT`, `币安人生USDT`, `我踏马来了USDT`, `牛来USDT`, `龙虾USDT`.

```bash
# 923 pares USDT com histórico publicado na Vision (522 em negociação + 401 delistados/pausados)
python scripts/download_binance.py --all-vision --interval 2h --market futures --full \
    --cache zip --jobs 5 --symbol-workers 8 --no-verify --no-combined
python scripts/backfill_gaps.py --dataset-dir data --market futures --interval 2h --jobs 6
```

Incluir os delistados é deliberado: elimina o viés de sobrevivência quando a estratégia for testada em universo amplo. Detalhes, contagem de candles e SHA-256 de cada um dos 923 pares ficam em `data/futures/2h/manifest.json`; o pacote completo é distribuído como asset da release para não inflar o repositório.

Os arquivos de dados brutos não devem ser commitados em massa no GitHub; prefira baixar localmente ou usar armazenamento de artefatos. Dados históricos não garantem desempenho futuro.
