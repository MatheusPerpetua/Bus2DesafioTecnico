# Desafio Técnico — Bus2

## Resumo
Pipeline ETL local que:
- Extrai dados dos CSVs (`empregados.csv`, `produtos.csv`, `vendas.csv`);
- Transforma aplicando regras de negócio (limpeza, merges e KPIs);
- Gera `resumo-vendas.parquet` (arquivo analítico) e `relatorio-preliminar.pdf` (relatório com gráficos);
- Carrega tabelas transformadas em um banco MySQL de teste.

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

│ ├─ inserirbanco.py # grava DataFrames no MySQL (expondo save_to_db)

│ └─ main.py # orquestrador do pipeline

├─ README.md

## Pré-requisitos
- Python 3.8+
- Criar um ambiente virtual
- MySQL acessível para carga (opcional para validação)
- Dependências: `pip install -r requirements/requirements.txt`

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
source venv/bin/activate
pip install -r requirements.txt
```
3. Execute o arquivo main.py:

```python src/main.py```
