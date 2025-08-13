# src/main.py
import logging, os
from extract import read_raw
from transform import transform_data
from report import save_parquet, build_pdf
from inserirbanco import save_raw_csvs_from_folder, save_transformed_to_dw

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    logging.info("Iniciando pipeline ETL local")

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    raw_folder = os.path.join(base, 'arquivos_teste_dados_bus2')
    outputs_dir = os.path.join(base, 'outputs')
    os.makedirs(outputs_dir, exist_ok=True)

    # 0) Persistir CSVs brutos no banco raw (ENGINE)
    save_raw_csvs_from_folder(raw_folder)

    # 1) Extração
    emp, prod, vendas = read_raw(raw_folder)

    # 2) Transformação
    resumo_dict = transform_data(emp, prod, vendas)

    # 3) Saídas locais (parquet + pdf)
    parquet_path = os.path.join(outputs_dir, 'resumo-vendas.parquet')
    pdf_path = os.path.join(outputs_dir, 'relatorio-preliminar.pdf')
    save_parquet(resumo_dict['resumo'], parquet_path)
    build_pdf(resumo_dict, pdf_path)

    # 4) Carga no Data Warehouse (transformados)
    save_transformed_to_dw(resumo_dict)

    logging.info("Pipeline finalizado com sucesso!")

if __name__ == '__main__':
    main()
