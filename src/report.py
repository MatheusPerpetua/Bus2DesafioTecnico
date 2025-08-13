import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os

def save_parquet(df, path_parquet):
    os.makedirs(os.path.dirname(path_parquet), exist_ok=True)
    # força conversão para tipos compatíveis
    df = df.copy()
    df = df.reset_index(drop=True)
    df.to_parquet(path_parquet, index=False)
    print(f"Parquet salvo: {path_parquet}")

def build_pdf(resumo_dict, output_pdf_path):
    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)
    with PdfPages(output_pdf_path) as pdf:
        # Página 1: KPIs simples
        total_vendas = resumo_dict['total_por_func']['total_vendas'].sum() if 'total_por_func' in resumo_dict else 0
        fig, ax = plt.subplots(figsize=(8,6))
        ax.axis('off')
        ax.text(0, 0.7, f"Total de Vendas (R$): {total_vendas:,.2f}", fontsize=14)
        ax.text(0, 0.5, f"Total de Funcionários: {len(resumo_dict['total_por_func']) if 'total_por_func' in resumo_dict else 0}", fontsize=12)
        pdf.savefig(fig)
        plt.close(fig)

        # Página 2: Top5 bar
        if 'top5' in resumo_dict and not resumo_dict['top5'].empty:
            top5 = resumo_dict['top5']
            fig, ax = plt.subplots(figsize=(8,6))
            ax.bar(top5['nome_emp'], top5['total_vendas'])
            ax.set_title('Top 5 Funcionários por Vendas')
            ax.set_ylabel('Total Vendas')
            plt.xticks(rotation=45, ha='right')
            pdf.savefig(fig)
            plt.close(fig)

        # Página 3: Vendas por categoria (se existir)
        if 'vendas_por_categoria' in resumo_dict and not resumo_dict['vendas_por_categoria'].empty:
            vc = resumo_dict['vendas_por_categoria']
            fig, ax = plt.subplots(figsize=(8,6))
            ax.pie(vc['valor_total'], labels=vc['categoria'], autopct='%1.1f%%', startangle=90)
            ax.set_title('Vendas por Categoria')
            pdf.savefig(fig)
            plt.close(fig)

    print(f"PDF gerado: {output_pdf_path}")
