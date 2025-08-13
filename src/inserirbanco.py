# src/inserirbanco.py
import os
import logging
import pandas as pd
import sys

# permitir imports a partir da raiz do projeto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# importa engines configuradas
try:
    # se seu config está em config/config.py
    from config import ENGINE, ENGINEDW
except Exception:
    # alternativa
    from config.config import ENGINE, ENGINEDW

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def insert_dataframe(df: pd.DataFrame, table_name: str, engine):
    """Insere um DataFrame no banco (replace)."""
    logging.info(f"Inserindo {len(df)} linhas na tabela `{table_name}`...")
    try:
        # limpa nomes de colunas para o banco (opcional)
        df_to_save = df.copy()
        # não forçar lower se preferir manter nomes originais no raw
        df_to_save.columns = df_to_save.columns.str.strip()
        df_to_save.to_sql(table_name, con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        logging.info(f"OK: {table_name}")
    except Exception as e:
        logging.exception(f"Erro ao inserir tabela {table_name}: {e}")
        raise


def save_raw_csvs_from_folder(raw_folder: str, engine=ENGINE):
    """
    Lê os CSVs brutos e salva no banco bruto (ENGINE).
    Nome das tabelas: empregados_raw, produtos_raw, vendas_raw
    (mantemos sufixo _raw para deixar claro que é a matéria-prima)
    """
    logging.info("Salvando CSVs brutos no banco raw (ENGINE)...")
    mapping = {
        'empregados.csv': 'empregados_raw',
        'produtos.csv': 'produtos_raw',
        'vendas.csv': 'vendas_raw'
    }
    for fname, table in mapping.items():
        csv_path = os.path.join(raw_folder, fname)
        if not os.path.exists(csv_path):
            logging.warning(f"Arquivo não encontrado (pulando): {csv_path}")
            continue
        try:
            df = pd.read_csv(csv_path)
            insert_dataframe(df, table, engine)
        except Exception as e:
            logging.exception(f"Erro ao inserir CSV {csv_path} -> {table}: {e}")


def save_transformed_to_dw(resumo_dict: dict, engine=ENGINEDW):
    """
    Salva os DataFrames transformados no DW.
    As chaves esperadas em resumo_dict (conforme seu transform.py):
      - 'dProdutos' -> tabela 'dim_produtos'
      - 'dFuncionarios' -> tabela 'dim_funcionarios'
      - 'fVendas' -> tabela 'fact_vendas'
      - 'resumo' -> tabela 'resumo_vendas' (granular enriquecido)
      - 'total_por_func' -> tabela 'total_por_func'
      - 'ticket_por_prod' -> tabela 'ticket_por_prod'
      - 'vendas_por_categoria' -> 'vendas_por_categoria'
      - 'top5' -> 'top5_func'
    """
    logging.info("Salvando dados transformados no Data Warehouse (ENGINEDW)...")
    mapping = {
        'dProdutos': 'dProdutos',
        'dFuncionarios': 'dFuncionarios',
        'fVendas': 'fVendas',
        'resumo': 'resumo_vendas',
        'total_por_func': 'total_por_func',
        'ticket_por_prod': 'ticket_por_prod',
        'vendas_por_categoria': 'vendas_por_categoria',
        'top5': 'top5_func'
    }

    for key, table in mapping.items():
        if key not in resumo_dict or resumo_dict[key] is None:
            logging.info(f"Chave '{key}' não encontrada em resumo_dict — pulando `{table}`.")
            continue
        df = resumo_dict[key].copy()
        # normalizar nomes (opcional) para DW: lower_case, sem espaços
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        # converter datetimes para formato compatível (se existir coluna 'data')
        if 'data' in df.columns:
            try:
                df['data'] = pd.to_datetime(df['data'], errors='coerce')
            except Exception:
                pass
        insert_dataframe(df, table, engine)


# execução standalone para facilitar testes locais (opcional)
if __name__ == "__main__":
    logging.info("Executando inserirbanco.py em modo standalone (ler CSVs e inserir no DB raw ENGINE).")
    raw_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'arquivos_teste_dados_bus2'))
    save_raw_csvs_from_folder(raw_folder, engine=ENGINE)
    logging.info("Modo standalone finalizado.")
