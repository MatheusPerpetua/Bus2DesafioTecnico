# src/main.py
import logging, os
from extract import read_raw
from transform import transform_data
from report import save_parquet, build_pdf
from inserirbanco import save_to_db

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    logging.info("Iniciando pipeline ETL local")

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    raw_folder = os.path.join(base, 'arquivos_teste_dados_bus2')
    outputs_dir = os.path.join(base, 'outputs')
    os.makedirs(outputs_dir, exist_ok=True)

    emp, prod, vendas = read_raw(raw_folder)
    resumo = transform_data(emp, prod, vendas)

    parquet_path = os.path.join(outputs_dir, 'resumo-vendas.parquet')
    pdf_path = os.path.join(outputs_dir, 'relatorio-preliminar.pdf')

    save_parquet(resumo['resumo'], parquet_path)
    build_pdf(resumo, pdf_path)

    save_to_db(resumo)

    logging.info("Pipeline finalizado com sucesso!")

if __name__ == '__main__':
    main()
