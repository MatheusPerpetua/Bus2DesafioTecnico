import os
import logging
import pandas as pd

# ajusta o path caso precise (você já tinha isso)
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import ENGINE, ENGINEDW

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def insert_dataframe(df, table_name, engine):
    """Insere um DataFrame no banco (replace)."""
    logging.info(f"Inserindo {len(df)} linhas na tabela `{table_name}`...")
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        logging.info(f"OK: {table_name}")
    except Exception as e:
        logging.exception(f"Erro ao inserir tabela {table_name}: {e}")
        raise

def save_to_db(resumo_dict, engine=ENGINEDW):
    """
    Recebe um dicionário com DataFrames (resumo, total_por_func, ticket_por_prod, vendas_por_categoria, top5)
    e grava em tabelas do banco.
    """
    mapping = {
        'dProdutos': prod,
        'dFuncionarios': emp,
        'fVendas': vendas,
        'resumo': 'resumo_vendas',
        'total_por_func': 'total_por_func',
        'ticket_por_prod': 'ticket_por_prod',
        'vendas_por_categoria': 'vendas_por_categoria',
        'top5': 'top5_func'
    } 

    for key, table in mapping.items():
        if key in resumo_dict and resumo_dict[key] is not None:
            df = resumo_dict[key]
            # garantir tipos básicos e nomes limpos
            df = df.copy()
            df.columns = df.columns.str.strip().str.lower()
            insert_dataframe(df, table, engine)
        else:
            logging.info(f"Chave '{key}' não encontrada em resumo_dict — pulando tabela `{table}`.")

# suporte para rodar o script isoladamente: lê CSVs e faz insert (comportamento legado)
if __name__ == "__main__":
    logging.info("Executando inserirbanco.py em modo standalone: lendo CSVs de 'arquivos_teste_dados_bus2' e inserindo no DB")
    table_load_order = ['empregados', 'produtos', 'vendas']
    for table_name in table_load_order:
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'arquivos_teste_dados_bus2', f'{table_name}.csv')
        csv_path = os.path.abspath(csv_path)
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                insert_dataframe(df, table_name, ENGINE)
            except Exception as e:
                logging.exception(f"Erro ao processar {csv_path}: {e}")
        else:
            logging.error(f"Arquivo CSV não encontrado: {csv_path}")
    logging.info("Fim do processo standalone.")
