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

## Baixar histórico
Python 3.10+; somente biblioteca padrão.

```bash
python scripts/download_binance.py --symbol BTCUSDT --interval 2h --months 12 --market futures
python scripts/download_binance.py --symbol ETHUSDT --interval 2h --months 12 --market futures
```

O padrão usa **12 meses calendários completos anteriores ao mês atual (o período exato de 18/11/2025 a 18/11/2026 exige complementar os meses parciais com dados diários)**; arquivos CSV são salvos em `data/raw/`. Dados públicos: https://data.binance.vision/ (repositório de documentação: https://github.com/binance/binance-public-data). Para mercado spot use `--market spot`. A periodicidade deve ser a mesma usada no TradingView.

Os arquivos de dados não devem ser commitados em massa no GitHub; prefira baixar localmente ou usar armazenamento de artefatos. Dados históricos não garantem desempenho futuro.
