import pandas as pd
from pathlib import Path

def read_raw(folder):
    p = Path(folder)
    emp = pd.read_csv(p/'empregados.csv')
    prod = pd.read_csv(p/'produtos.csv')
    vendas = pd.read_csv(p/'vendas.csv')
    return emp, prod, vendas
