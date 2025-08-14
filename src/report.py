import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter

# --- CONFIGURAÇÕES GLOBAIS DE LAYOUT E ESTILO ---
# Tamanho da página A4 landscape em polegadas (11.69 x 8.27)
FIGSIZE = (12, 10) # Ajustado para A4 landscape

# Margens da página (proporcional ao tamanho da figura)
MARGINS = {
    'left': 0.08, 'right': 0.92,  # Aumenta margens laterais
    'top': 0.90, 'bottom': 0.10   # Ajusta margens superior e inferior
}

# Área de conteúdo dentro das margens: [left, bottom, width, height]
CONTENT_BOX = {
    'full': [MARGINS['left'], MARGINS['bottom'], MARGINS['right'] - MARGINS['left'], MARGINS['top'] - MARGINS['bottom']]
}

# Fontes
TITLE_FONT = 18
SUBTITLE_FONT = 14
TEXT_FONT = 10
LABEL_FONT = 9
TABLE_FONT = 8

# Cores (paleta mais profissional)
COLOR_PRIMARY = '#2E86C1'  # Azul corporativo
COLOR_SECONDARY = '#6AB04C' # Verde complementar
COLOR_ACCENT = '#FFC300'    # Amarelo para destaque
COLOR_TEXT = '#333333'      # Cinza escuro para texto
COLOR_LIGHT_GRAY = '#CCCCCC' # Cinza claro para linhas/bordas

# --- FUNÇÕES AUXILIARES ---

def currency_fmt(x, pos=None):
    """Formata um número como valor monetário em BRL."""
    try:
        # Garante que o valor é numérico antes de formatar
        if pd.isna(x): return ""
        return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(x)

def _page_footer(fig, page_num):
    """Adiciona um rodapé com o número da página."""
    fig.text(0.90, 0.04, f"Página {page_num}", ha='right', va='bottom', fontsize=8, color=COLOR_LIGHT_GRAY)
    fig.text(0.10, 0.04, "Relatório de Análise de Dados Bus2", ha='left', va='bottom', fontsize=8, color=COLOR_LIGHT_GRAY)

def _annotate_barh(ax, y_pos, values, fmt_func=currency_fmt, inside_threshold=0.12):
    """Adiciona rótulos de valor às barras horizontais."""
    maxv = max(values) if len(values) > 0 else 0.0
    pad = maxv * 0.01 if maxv > 0 else 1.0
    for i, v in enumerate(values):
        ypos = y_pos[i]
        label = fmt_func(v)
        if maxv > 0 and v / maxv >= inside_threshold:
            ax.text(v - pad, ypos, label, va='center', ha='right', fontsize=LABEL_FONT, color='white')
        else:
            ax.text(v + pad, ypos, label, va='center', ha='left', fontsize=LABEL_FONT, color=COLOR_TEXT)

def _new_page(fig, title=None, subtitle=None):
    """
    Cria um novo canvas com margens padronizadas e adiciona título/subtítulo.
    Retorna uma axes 'content' ocupando a área interna.
    """
    fig.subplots_adjust(**MARGINS)
    if title:
        fig.suptitle(title, fontsize=TITLE_FONT, y=0.96, color=COLOR_TEXT, weight='bold')
    if subtitle:
        fig.text(0.5, 0.92, subtitle, ha='center', va='top', fontsize=SUBTITLE_FONT, color=COLOR_TEXT)
    # content axes (invisível, para coordenadas)
    content = fig.add_axes(CONTENT_BOX['full'])
    content.axis('off')
    return content

def save_parquet(df, path_parquet):
    """Salva um DataFrame em formato Parquet."""
    os.makedirs(os.path.dirname(path_parquet), exist_ok=True)
    df = df.reset_index(drop=True)
    df.to_parquet(path_parquet, index=False)
    print(f"Parquet salvo: {path_parquet}")

def build_pdf(resumo_dict, output_pdf_path, top_n_employees=10, top_n_products=12):
    """Constrói o relatório PDF com base nos dados processados."""
    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)

    resumo = resumo_dict.get('resumo', pd.DataFrame())
    total_por_func = resumo_dict.get('total_por_func', pd.DataFrame())
    ticket_por_prod = resumo_dict.get('ticket_por_prod', pd.DataFrame())
    vendas_por_categoria = resumo_dict.get('vendas_por_categoria', pd.DataFrame())
    quality = resumo_dict.get('quality_metrics', {})

    # KPIs
    total_vendas = resumo['valor_total'].sum() if not resumo.empty and 'valor_total' in resumo.columns else 0.0
    n_transacoes = len(resumo)
    ticket_medio = resumo['valor_total'].mean() if not resumo.empty and 'valor_total' in resumo.columns else 0.0
    ticket_mediano = resumo['valor_total'].median() if not resumo.empty and 'valor_total' in resumo.columns else 0.0
    n_produtos = resumo['id_produto'].nunique() if 'id_produto' in resumo.columns else 0
    n_funcionarios = resumo['id_empregado'].nunique() if 'id_empregado' in resumo.columns else 0

    page = 1
    with PdfPages(output_pdf_path) as pdf:
        # --- PAGE 1: COVER + KPIs ---
        fig = plt.figure(figsize=FIGSIZE)
        _new_page(fig, title="Relatório de Análise de Vendas", subtitle="Desafio Técnico Bus2")
        
        fig.text(0.10, 0.80, "Autor: Matheus Vieira Perpetua", fontsize=SUBTITLE_FONT, color=COLOR_TEXT)
        fig.text(0.10, 0.75, f"Data de Geração: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}", fontsize=TEXT_FONT, color=COLOR_TEXT)

        # KPI cards (2x3 grid)
        kpis = [
            ("Total Vendas (R$)", total_vendas),
            ("Transações (count)", n_transacoes),
            ("Ticket Médio (R$)", ticket_medio),
            ("Ticket Mediano (R$)", ticket_mediano),
            ("Produtos Únicos", n_produtos),
            ("Funcionários Únicos", n_funcionarios)
        ]
        
        for i, (title, value) in enumerate(kpis):
            x_start = 0.10 + (i % 3) * 0.28  # Ajusta espaçamento horizontal
            y_start = 0.55 - (i // 3) * 0.18  # Ajusta espaçamento vertical
            
            fig.text(x_start, y_start + 0.04, title, fontsize=TEXT_FONT, weight='bold', color=COLOR_TEXT)
            
            # Formatação específica para valores
            if "Vendas" in title or "Ticket" in title:
                formatted_value = currency_fmt(value)
            else:
                formatted_value = f"{value:,}".replace(",", ".") # Formata números grandes
            
            fig.text(x_start, y_start, formatted_value, fontsize=SUBTITLE_FONT, weight='bold', color=COLOR_PRIMARY)
        
        _page_footer(fig, page)
        pdf.savefig(fig)
        plt.close(fig)
        page += 1

        # --- PAGE 2: Top employees (bar + table) ---
        if not total_por_func.empty:
            df_emp = total_por_func.copy().sort_values('total_vendas', ascending=False).head(top_n_employees)
            # Renomear coluna para exibição no PDF se necessário
            if 'nome_emp' in df_emp.columns: # Verifica se a coluna existe
                df_emp.rename(columns={'nome_emp': 'Nome do Funcionário'}, inplace=True)
            elif 'nome_empregado' in df_emp.columns:
                df_emp.rename(columns={'nome_empregado': 'Nome do Funcionário'}, inplace=True)
           
            
            names = df_emp['Nome do Funcionário'].astype(str).values
            values = df_emp['total_vendas'].astype(float).values
            y_pos = np.arange(len(names))

            fig = plt.figure(figsize=FIGSIZE)
            _new_page(fig, title=f"Top {top_n_employees} Funcionários por Volume de Vendas")

            left, bottom, width, height = CONTENT_BOX['full']
            ax_bar = fig.add_axes([left, bottom + 0.25, width, 0.60]) # Gráfico ocupa mais espaço
            ax_tab = fig.add_axes([left, bottom, width, 0.20]) # Tabela abaixo do gráfico

            ax_bar.barh(y_pos, values, align='center', color=COLOR_PRIMARY)
            ax_bar.invert_yaxis()
            ax_bar.set_yticks(y_pos)
            ax_bar.set_yticklabels(names, fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax_bar.set_xlabel("Tabela top 5 Vendas (R$) por funcionário", fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax_bar.xaxis.set_major_formatter(FuncFormatter(currency_fmt))
            ax_bar.tick_params(axis='x', colors=COLOR_TEXT)
            ax_bar.tick_params(axis='y', colors=COLOR_TEXT)
            ax_bar.spines['top'].set_visible(False)
            ax_bar.spines['right'].set_visible(False)
            ax_bar.spines['left'].set_color(COLOR_LIGHT_GRAY)
            ax_bar.spines['bottom'].set_color(COLOR_LIGHT_GRAY)
            ax_bar.grid(axis='x', linestyle='--', alpha=0.7, color=COLOR_LIGHT_GRAY)

            if len(values) > 0:
                ax_bar.set_xlim(0, max(values) * 1.15) # Aumenta o limite para rótulos externos
            _annotate_barh(ax_bar, y_pos, values, fmt_func=currency_fmt, inside_threshold=0.14)

            # Tabela top 5 (separada para melhor formatação)
            t5 = df_emp.head(5)[['Nome do Funcionário', 'total_vendas']].copy()
            t5['total_vendas'] = t5['total_vendas'].apply(lambda x: currency_fmt(x))
            
            ax_tab.axis('off')
            table = ax_tab.table(cellText=t5.values, colLabels=t5.columns, cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
            table.auto_set_font_size(False)
            table.set_fontsize(TABLE_FONT)
            table.scale(1, 1.5) # Aumenta altura das células
            
            for (row, col), cell in table.get_celld().items():
                cell.set_height(0.15) # Ajusta altura da célula
                cell.set_text_props(color=COLOR_TEXT)
                cell.set_edgecolor(COLOR_LIGHT_GRAY)
                if row == 0: # Cabeçalho
                    cell.set_facecolor(COLOR_PRIMARY)
                    cell.set_text_props(weight='bold', color='white')
                else:
                    cell.set_facecolor('white')

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1

        # --- PAGE 3: Ticket medio por produto (bar + histogram) ---
        if not ticket_por_prod.empty:
            df_ticket = ticket_por_prod.copy().sort_values('ticket_medio', ascending=False).head(top_n_products)
            if 'nome_prod' in df_ticket.columns:
                df_ticket.rename(columns={'nome_prod': 'Nome do Produto'}, inplace=True)
            elif 'nome_produto' in df_ticket.columns:
                df_ticket.rename(columns={'nome_produto': 'Nome do Produto'}, inplace=True)

            names = df_ticket['Nome do Produto'].astype(str).values
            vals = df_ticket['ticket_medio'].astype(float).values
            y_pos = np.arange(len(names))

            fig = plt.figure(figsize=FIGSIZE)
            _new_page(fig, title=f"Top {top_n_products} Produtos por Ticket Médio")

            left, bottom, width, height = CONTENT_BOX['full']
            ax_top = fig.add_axes([left, bottom + 0.35, width, 0.50]) # Gráfico ocupa mais espaço
            ax_hist = fig.add_axes([left, bottom + 0.08, width, 0.25]) # Histograma abaixo

            # BARRAS HORIZONTAIS (ticket médio)
            ax_top.barh(y_pos, vals, align='center', color=COLOR_PRIMARY)
            ax_top.invert_yaxis()
            ax_top.set_yticks(y_pos)
            ax_top.set_yticklabels(names, fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax_top.set_xlabel("Ticket Médio (R$)", fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax_top.xaxis.set_major_formatter(FuncFormatter(currency_fmt))
            ax_top.tick_params(axis='x', colors=COLOR_TEXT)
            ax_top.tick_params(axis='y', colors=COLOR_TEXT)
            ax_top.spines['top'].set_visible(False)
            ax_top.spines['right'].set_visible(False)
            ax_top.spines['left'].set_color(COLOR_LIGHT_GRAY)
            ax_top.spines['bottom'].set_color(COLOR_LIGHT_GRAY)
            ax_top.grid(axis='x', linestyle='--', alpha=0.7, color=COLOR_LIGHT_GRAY)

            if len(vals) > 0:
                ax_top.set_xlim(0, max(vals) * 1.15)
            _annotate_barh(ax_top, y_pos, vals, fmt_func=currency_fmt, inside_threshold=0.12)
            ax_top.set_title(f"Ticket Médio por Produto", fontsize=SUBTITLE_FONT, color=COLOR_TEXT)

            # HISTOGRAMA (distribuição)
            ax_hist.hist(resumo['valor_total'].dropna(), bins=30, color=COLOR_SECONDARY, edgecolor='white')
            ax_hist.set_title("Distribuição de Valor por Transação", fontsize=SUBTITLE_FONT, color=COLOR_TEXT)
            ax_hist.set_xlabel("Valor Total (R$)", fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax_hist.set_ylabel("Frequência", fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax_hist.xaxis.set_major_formatter(FuncFormatter(currency_fmt))
            ax_hist.tick_params(axis='x', rotation=0, colors=COLOR_TEXT)
            ax_hist.tick_params(axis='y', colors=COLOR_TEXT)
            ax_hist.spines['top'].set_visible(False)
            ax_hist.spines['right'].set_visible(False)
            ax_hist.spines['left'].set_color(COLOR_LIGHT_GRAY)
            ax_hist.spines['bottom'].set_color(COLOR_LIGHT_GRAY)
            ax_hist.grid(axis='y', linestyle='--', alpha=0.7, color=COLOR_LIGHT_GRAY)

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1

        # --- PAGE 4: Vendas por categoria (padronizado) ---
        if not vendas_por_categoria.empty:
            vc = vendas_por_categoria.sort_values('valor_total', ascending=False) # Corrigido para 'valor_total'
            if 'categoria' in vc.columns: # Verifica se a coluna existe
                vc.rename(columns={'categoria': 'Categoria'}, inplace=True)
            if 'valor_total' in vc.columns: # Corrigido para 'valor_total'
                vc.rename(columns={'valor_total': 'Valor Total'}, inplace=True)

            fig = plt.figure(figsize=FIGSIZE)
            _new_page(fig, title="Vendas por Categoria de Produto")

            left, bottom, width, height = CONTENT_BOX['full']
            ax1 = fig.add_axes([left, bottom + 0.15, width * 0.60, 0.70]) # Gráfico de barras maior
            ax2 = fig.add_axes([left + width * 0.65, bottom + 0.25, width * 0.30, 0.50]) # Gráfico de pizza menor

            ax1.bar(vc['Categoria'].astype(str), vc['Valor Total'], color=COLOR_PRIMARY)
            ax1.set_xticklabels(vc['Categoria'].astype(str), rotation=45, ha='right', fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax1.set_ylabel("Valor (R$)", fontsize=LABEL_FONT, color=COLOR_TEXT)
            ax1.yaxis.set_major_formatter(FuncFormatter(currency_fmt))
            ax1.tick_params(axis='x', colors=COLOR_TEXT)
            ax1.tick_params(axis='y', colors=COLOR_TEXT)
            ax1.spines['top'].set_visible(False)
            ax1.spines['right'].set_visible(False)
            ax1.spines['left'].set_color(COLOR_LIGHT_GRAY)
            ax1.spines['bottom'].set_color(COLOR_LIGHT_GRAY)
            ax1.grid(axis='y', linestyle='--', alpha=0.7, color=COLOR_LIGHT_GRAY)
            #ax1.set_title("Total de Vendas por Categoria", fontsize=SUBTITLE_FONT, color=COLOR_TEXT)

            # Gráfico de pizza
            wedges, texts, autotexts = ax2.pie(vc['Valor Total'], labels=vc['Categoria'], autopct='%1.1f%%', startangle=90, 
                                            textprops={'fontsize': LABEL_FONT, 'color': COLOR_TEXT}, 
                                            colors=plt.cm.Paired.colors) # Usando um colormap diferente
            for autotext in autotexts:
                autotext.set_color('white') # Cor do texto dentro das fatias
            ax2.set_title("Participação de Vendas por Categoria", fontsize=SUBTITLE_FONT, color=COLOR_TEXT)

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1

        # --- PAGE 5: Série temporal (padronizado) ---
        if 'data' in resumo.columns:
            tmp = resumo.copy()
            tmp['data'] = pd.to_datetime(tmp['data'], errors='coerce')
            tmp = tmp.dropna(subset=['data'])
            if not tmp.empty:
                series = tmp.set_index('data').resample('M')['valor_total'].sum() # Corrigido para 'valor_total'
                fig = plt.figure(figsize=FIGSIZE)
                _new_page(fig, title="Evolução Mensal das Vendas")

                left, bottom, width, height = CONTENT_BOX['full']
                ax = fig.add_axes([left, bottom + 0.15, width, 0.70])
                ax.plot(series.index, series.values, marker='o', linewidth=2, color=COLOR_PRIMARY, markersize=5)
                ax.set_xlabel("Mês", fontsize=LABEL_FONT, color=COLOR_TEXT)
                ax.set_ylabel("Valor Total (R$)", fontsize=LABEL_FONT, color=COLOR_TEXT)
                ax.yaxis.set_major_formatter(FuncFormatter(currency_fmt))
                ax.tick_params(axis='x', colors=COLOR_TEXT)
                ax.tick_params(axis='y', colors=COLOR_TEXT)
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['left'].set_color(COLOR_LIGHT_GRAY)
                ax.spines['bottom'].set_color(COLOR_LIGHT_GRAY)
                ax.grid(True, linestyle='--', alpha=0.7, color=COLOR_LIGHT_GRAY)

                _page_footer(fig, page)
                pdf.savefig(fig)
                plt.close(fig)
                page += 1

        # --- PAGE 6: Qualidade dos dados (texto formatado e sem sobreposição) ---
        fig = plt.figure(figsize=FIGSIZE)
        _new_page(fig, title="Qualidade dos Dados - Resumo")
        left, bottom, width, height = CONTENT_BOX['full']
        ax_text = fig.add_axes([left + 0.01, bottom + 0.02, width - 0.02, height - 0.15])
        ax_text.axis('off')

        lines = []
        lines.append("**Métricas de Qualidade dos Dados:**\n")
        if quality:
            for k,v in quality.items():
                lines.append(f"- **{k}:** {v:,}" if isinstance(v, int) else f"- **{k}:** {v}")
        else:
            if not resumo.empty:
                missing = {c: int(resumo[c].isna().sum()) for c in resumo.columns}
                dup_v = int(resumo.duplicated().sum())
                lines.append(f"- **Total de registros no resumo:** {len(resumo):,}")
                lines.append(f"- **Registros duplicados (resumo):** {dup_v:,}")
                lines.append("\n**Contagem de Valores Ausentes por Coluna (Top 12):**")
                nonzero = [(c,n) for c,n in missing.items() if n>0]
                if nonzero:
                    for c,n in nonzero[:12]:
                        lines.append(f"  - **{c}:** {n:,}")
                else:
                    for c,n in list(missing.items())[:12]:
                        lines.append(f"  - **{c}:** {n:,}")
            else:
                lines.append("- Não há dados no resumo para análise de qualidade.")
        
        # Usar text_wrap para quebrar linhas automaticamente
        text_content = "\n".join(lines).replace(",", ".") # Substitui vírgula por ponto para números grandes
        ax_text.text(0, 1, text_content, fontsize=TEXT_FONT, family='monospace', va='top', wrap=True, color=COLOR_TEXT)
        
        _page_footer(fig, page)
        pdf.savefig(fig)
        plt.close(fig)
        page += 1

        # --- PAGE 7: Amostra registros (tabela) ---
        if not resumo.empty:
            sample = resumo.head(12).copy()
            cols_priority = ['id_venda','data','id_produto','id_empregado','quantidade','valor_unitario','valor_total','nome_emp','nome_prod','categoria'] # Corrigido para 'valor_total'
            cols = [c for c in cols_priority if c in sample.columns]
            sample = sample[cols]
            
            # Formatação de valores monetários e datas
            for col in ['valor_unitario','valor_total']:
                if col in sample.columns:
                    sample[col] = sample[col].apply(lambda x: currency_fmt(x) if pd.notna(x) else "")
            if 'quantidade' in sample.columns:
                sample['quantidade'] = sample['quantidade'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "")
            if 'data' in sample.columns:
                sample['data'] = pd.to_datetime(sample['data'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')

            fig = plt.figure(figsize=FIGSIZE)
            _new_page(fig, title="Amostra de Registros (Dados Transformados)")
            left, bottom, width, height = CONTENT_BOX['full']
            ax_table = fig.add_axes([left, bottom, width, height])
            ax_table.axis('off')
            
            # Ajustes para a tabela
            table = ax_table.table(cellText=sample.values, colLabels=sample.columns, cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
            table.auto_set_font_size(False)
            table.set_fontsize(TABLE_FONT)
            table.scale(1, 1.5) # Aumenta altura das células
            
            for (row, col), cell in table.get_celld().items():
                cell.set_height(0.15) # Ajusta altura da célula
                cell.set_text_props(color=COLOR_TEXT)
                cell.set_edgecolor(COLOR_LIGHT_GRAY)
                if row == 0: # Cabeçalho
                    cell.set_facecolor(COLOR_PRIMARY)
                    cell.set_text_props(weight='bold', color='white')
                else:
                    cell.set_facecolor('white')

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1

    print(f"PDF gerado: {output_pdf_path}")
