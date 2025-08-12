# Bus2 - Desafio Técnico (Matheus Vieira Perpetua)

## Resumo
Pipeline ETL local que extrai CSVs (empregados, produtos, vendas), transforma os dados, gera `resumo-vendas.parquet` e `relatorio-final.pdf` e carrega tabelas no MySQL.

## Como rodar (ambiente Linux)
1. Ativar ambiente virtual:
   ```bash
   source venv/bin/activate
   pip install -r requirements.txt
