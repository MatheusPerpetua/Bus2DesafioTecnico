# src/transform.py
import pandas as pd

def transform_data(emp, prod, vendas):
    """
    Transformações específicas para os CSVs fornecidos:
    - emp: empregados.csv (id_empregado, nome, cargo, idade)
    - prod: produtos.csv (id_produto, nome, preco, categoria)
    - vendas: vendas.csv (id_venda, data, id_produto, id_empregado, quantidade, valor_unitario, valor_total)
    Retorna um dict com DataFrames: resumo, total_por_func, ticket_por_prod, vendas_por_categoria, top5
    """

    # Trabalhar em cópias para evitar SettingWithCopyWarning
    emp = emp.copy()
    prod = prod.copy()
    vendas = vendas.copy()

    # Normalizar nomes (remover espaços)
    emp.columns = emp.columns.str.strip()
    prod.columns = prod.columns.str.strip()
    vendas.columns = vendas.columns.str.strip()

    # Limpar duplicados
    emp = emp.drop_duplicates().reset_index(drop=True)
    prod = prod.drop_duplicates().reset_index(drop=True)
    vendas = vendas.drop_duplicates().reset_index(drop=True)

    # Garantir tipos numéricos nas colunas críticas
    vendas['quantidade'] = pd.to_numeric(vendas['quantidade'], errors='coerce').fillna(0)
    vendas['valor_unitario'] = pd.to_numeric(vendas['valor_unitario'], errors='coerce').fillna(0)
    vendas['valor_total'] = pd.to_numeric(vendas['valor_total'], errors='coerce').fillna(0)

    # Padronizar chaves para merge: vamos usar id_empregado, id_produto nas vendas
    # Nos datasets emp/prod, renomear colunas id para 'id_empregado' / 'id_produto' se necessário
    if 'id_empregado' not in emp.columns:
        # tenta renomear coluna que contém 'id' no nome
        for c in emp.columns:
            if 'id' in c.lower():
                emp = emp.rename(columns={c: 'id_empregado'})
                break

    if 'id_produto' not in prod.columns:
        for c in prod.columns:
            if 'id' in c.lower():
                prod = prod.rename(columns={c: 'id_produto'})
                break

    # Se os nomes de nome nas tabelas forem ambíguos, renomear para 'nome_emp' e 'nome_prod'
    # para evitar colisões após merge
    emp = emp.rename(columns={col:('nome_emp' if col.lower()=='nome' else col) for col in emp.columns})
    prod = prod.rename(columns={col:('nome_prod' if col.lower()=='nome' else col) for col in prod.columns})

    # Merge das tabelas
    # vendas.id_empregado -> emp.id_empregado
    df = vendas.merge(emp, left_on='id_empregado', right_on='id_empregado', how='left', suffixes=('','_emp'))
    df = df.merge(prod, left_on='id_produto', right_on='id_produto', how='left', suffixes=('','_prod'))

    # Se valor_total não existir ou estiver zerado, calcula a partir de quantidade * valor_unitario
    if 'valor_total' not in df.columns or df['valor_total'].isna().all() or (df['valor_total']==0).all():
        df['valor_total'] = df['quantidade'] * df['valor_unitario']

    # KPI: total de vendas por funcionário
    # usar 'nome_emp' se existir, senão usar coluna 'nome' que pode existir
    nome_emp_col = 'nome_emp' if 'nome_emp' in df.columns else next((c for c in df.columns if 'nome' in c.lower()), None)
    total_por_func = df.groupby(['id_empregado', nome_emp_col])['valor_total'].sum().reset_index().rename(columns={'valor_total':'total_vendas', nome_emp_col:'nome_emp'})

    # Ticket médio por produto = total_venda_por_produto / total_quantidade_vendida
    ticket_sum = df.groupby(['id_produto', 'nome_prod'])['valor_total'].sum().reset_index()
    vendas_qt = df.groupby('id_produto')['quantidade'].sum().reset_index().rename(columns={'quantidade':'total_qt'})
    ticket_por_prod = ticket_sum.merge(vendas_qt, on='id_produto')
    ticket_por_prod['ticket_medio'] = ticket_por_prod['valor_total'] / ticket_por_prod['total_qt'].replace({0:1})

    # Vendas por categoria (produtos tem coluna 'categoria')
    if 'categoria' in df.columns:
        vendas_por_categoria = df.groupby('categoria')['valor_total'].sum().reset_index().rename(columns={'valor_total':'valor_total'})
    else:
        vendas_por_categoria = pd.DataFrame(columns=['categoria','valor_total'])

    # Top 5 funcionários por volume de vendas
    top5 = total_por_func.sort_values('total_vendas', ascending=False).head(5)

    # Resumo granular (linhas de vendas enriquecidas)
    resumo = df.copy()

    # Garantir tipos e limpar colunas que podem conter objetos complexos antes de escrever parquet/db
    # (converter datas)
    if 'data' in resumo.columns:
        try:
            resumo['data'] = pd.to_datetime(resumo['data'], errors='coerce')
        except Exception:
            pass

    return {
        'dProdutos': prod,
        'dFuncionarios': emp,
        'fVendas': vendas,
        'resumo': resumo,
        'total_por_func': total_por_func,
        'ticket_por_prod': ticket_por_prod,
        'vendas_por_categoria': vendas_por_categoria,
        'top5': top5
    }
