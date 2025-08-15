#Desafio Técnico — Bus2

Visão geral
Este repositório contém um pipeline ETL local (extração → transformação → carga) construído para o desafio da Bus2. O pipeline lê CSVs brutos, aplica regras de negócio, gera um arquivo analítico (Parquet), produz um relatório em PDF e insere as tabelas transformadas em um banco MySQL/DW.

Objetivos cobridos

Ler os CSVs: empregados.csv, produtos.csv, vendas.csv

Limpeza, padronização e agregações (KPIs)

Gerar outputs/resumo-vendas.parquet e outputs/relatorio-preliminar.pdf

Carregar tabelas no MySQL (DB de teste) e no DW (se configurado)

## Estrutura do repositório

Bus2DesafioTecnico/

├─ arquivos_teste_dados_bus2/ # CSVs brutos

├─ config/

│ └─ config.py # Engine SQLAlchemy + pymysql

├─ outputs/ # arquivos gerados

├─ requirements/requirements.txt

├─ src/

│ ├─ extract.py # leitura dos CSVs

│ ├─ transform.py # regras de negócio / agregações

│ ├─ report.py # grava parquet e gera PDF

│ ├─ inserirbanco.py # grava DataFrames no MySQL 

│ └─ main.py # orquestrador do pipeline

├─ README.md

## Pré-requisitos

-Python 3.8+

-Sistema operacional: Linux / Windows (com ajustes)

-MySQL remoto ou local para testar a carga

-Recomendado: ambiente virtual (venv)

## Configuração (.env)
Crie um arquivo .env na raiz com:

HOST= Seu host

DATABASE=Bus2_analise

DATABASEDW=Bus2_analiseDW

USER_DB= Seu usuário

PASSWORDDB=SuaSenhaSegura

PORTA= Sua porta

## Como rodar (passo a passo)
1. Clone este repositório.

2. Vá até a raiz, instale e ative o ambiente virtual e após isso instale dependências:
```bash

python -m venv venv
source venv/bin/activate        # Linux / macOS
# venv\Scripts\activate         # Windows PowerShell
pip install -r requirements/requirements.txt
```
3. Execute o arquivo main.py:

```python src/main.py```
