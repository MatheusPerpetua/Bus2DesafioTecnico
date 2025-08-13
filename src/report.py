# src/report.py  (substituir arquivo atual)
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter

# Padronização de layout
FIGSIZE = (18.00, 11.00)      # A4 landscape
MARGINS = {'left': 0.06, 'right': 0.98, 'top': 0.92, 'bottom': 0.06}
CONTENT_BOX = {
    # content area inside the margins: [left, bottom, width, height]
    'full': [MARGINS['left'], MARGINS['bottom'], MARGINS['right'] - MARGINS['left'], MARGINS['top'] - MARGINS['bottom']]
}
TITLE_FONT = 16
SUBTITLE_FONT = 12
TEXT_FONT = 10
LABEL_FONT = 9
TABLE_FONT = 8

def currency_fmt(x, pos=None):
    try:
        return f"R$ {x:,.2f}"
    except:
        return x

def _page_footer(fig, page_num):
    fig.text(0.99, 0.01, f"Página {page_num}", ha='right', va='bottom', fontsize=8, color='gray')

def _annotate_barh(ax, y_pos, values, fmt_func=currency_fmt, inside_threshold=0.12):
    maxv = max(values) if len(values) > 0 else 0.0
    pad = maxv * 0.01 if maxv>0 else 1.0
    for i, v in enumerate(values):
        ypos = y_pos[i]
        label = fmt_func(v)
        if maxv>0 and v/maxv >= inside_threshold:
            ax.text(v - pad, ypos, label, va='center', ha='right', fontsize=LABEL_FONT, color='white')
        else:
            ax.text(v + pad, ypos, label, va='center', ha='left', fontsize=LABEL_FONT, color='black')

def save_parquet(df, path_parquet):
    os.makedirs(os.path.dirname(path_parquet), exist_ok=True)
    df = df.reset_index(drop=True)
    df.to_parquet(path_parquet, index=False)
    print(f"Parquet salvo: {path_parquet}")

def _new_page(fig, title=None):
    """
    Cria um canvas com mesmas margens. Retorna uma axes 'content' ocupando area interna.
    Use ax = fig.add_axes(CONTENT_BOX['full']) para adicionar conteúdo.
    """
    # já usamos fig.subplots_adjust globalmente no main loop
    if title:
        fig.suptitle(title, fontsize=TITLE_FONT, y=0.96)
    # content axes (invisível, para coordenadas)
    content = fig.add_axes(CONTENT_BOX['full'])
    content.axis('off')
    return content

def build_pdf(resumo_dict, output_pdf_path, top_n_employees=10, top_n_products=12):
    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)

    resumo = resumo_dict.get('resumo', pd.DataFrame())
    total_por_func = resumo_dict.get('total_por_func', pd.DataFrame())
    ticket_por_prod = resumo_dict.get('ticket_por_prod', pd.DataFrame())
    vendas_por_categoria = resumo_dict.get('vendas_por_categoria', pd.DataFrame())
    quality = resumo_dict.get('quality_metrics', {})

    # KPIs
    total_vendas = resumo['valor_total'].sum() if not resumo.empty else 0.0
    n_transacoes = len(resumo)
    ticket_medio = resumo['valor_total'].mean() if not resumo.empty else 0.0
    ticket_mediano = resumo['valor_total'].median() if not resumo.empty else 0.0
    n_produtos = resumo['id_produto'].nunique() if 'id_produto' in resumo.columns else 0
    n_funcionarios = resumo['id_empregado'].nunique() if 'id_empregado' in resumo.columns else 0

    page = 1
    with PdfPages(output_pdf_path) as pdf:
        # --- PAGE 1: COVER + KPIs (uniform frame) ---
        fig = plt.figure(figsize=FIGSIZE)
        fig.subplots_adjust(**MARGINS)
        _new_page(fig, title="Relatório Final - Desafio Técnico")
        fig.text(0.02, 0.88, "Autor: Matheus Vieira Perpetua", fontsize=TEXT_FONT)
        # KPI cards (2x3)
        kpis = [
            ("Total Vendas (R$)", currency_fmt(total_vendas)),
            ("Transações (count)", f"{n_transacoes:,}"),
            ("Ticket médio (R$)", currency_fmt(ticket_medio)),
            ("Ticket mediano (R$)", currency_fmt(ticket_mediano)),
            ("Produtos únicos", f"{n_produtos:,}"),
            ("Funcionários únicos", f"{n_funcionarios:,}")
        ]
        for i, (title, value) in enumerate(kpis):
            x = 0.06 + (i % 3) * 0.30
            y = 0.65 - (i // 3) * 0.16
            fig.text(x, y+0.03, title, fontsize=10, weight='bold')
            fig.text(x, y, value, fontsize=14, weight='bold')
        _page_footer(fig, page)
        pdf.savefig(fig)   # sem bbox_inches='tight' -> mantém padronização
        plt.close(fig)
        page += 1

        # --- PAGE 2: Top employees (bar + table) ---
        if not total_por_func.empty:
            df_emp = total_por_func.copy().sort_values('total_vendas', ascending=False).head(top_n_employees)
            names = df_emp['nome_emp'].astype(str).values
            values = df_emp['total_vendas'].astype(float).values
            y_pos = np.arange(len(names))

            fig = plt.figure(figsize=FIGSIZE)
            fig.subplots_adjust(**MARGINS)
            _new_page(fig, title=f"Top {top_n_employees} - Total de Vendas por Funcionário")

            # criar axes para o gráfico ocupando 70% da altura do content box
            left, bottom, width, height = CONTENT_BOX['full']
            ax_bar = fig.add_axes([left, bottom+0.22, width, 0.60])
            ax_tab = fig.add_axes([left, bottom, width, 0.18])

            ax_bar.barh(y_pos, values, align='center', color='#2E86C1')
            ax_bar.invert_yaxis()
            ax_bar.set_yticks(y_pos)
            ax_bar.set_yticklabels(names, fontsize=9)
            ax_bar.set_xlabel("Total Vendas (R$)")
            ax_bar.xaxis.set_major_formatter(FuncFormatter(currency_fmt))
            if len(values) > 0:
                ax_bar.set_xlim(0, max(values)*1.12)
            _annotate_barh(ax_bar, y_pos, values, fmt_func=currency_fmt, inside_threshold=0.14)

            # tabela top5
            t5 = df_emp.head(5)[['nome_emp','total_vendas']].copy()
            t5['total_vendas'] = t5['total_vendas'].apply(lambda x: f"{x:,.2f}")
            ax_tab.axis('off')
            table = ax_tab.table(cellText=t5.values, colLabels=['nome_emp','total_vendas'], cellLoc='left', loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(TABLE_FONT)
            table.scale(1, 1.2)

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1

                # --- PAGE 3: Ticket medio por produto (bar + histogram) - corrigido espaçamento ---
        if not ticket_por_prod.empty:
            df_ticket = ticket_por_prod.copy().sort_values('ticket_medio', ascending=False).head(top_n_products)
            names = df_ticket['nome_prod'].astype(str).values
            vals = df_ticket['ticket_medio'].astype(float).values
            y_pos = np.arange(len(names))

            fig = plt.figure(figsize=FIGSIZE)
            fig.subplots_adjust(**MARGINS)
            _new_page(fig, title=f"Top {top_n_products} - Ticket Médio por Produto")

            # Definir áreas com mais espaçamento: top ocupa menor altura, hist maior área inferior
            left, bottom, width, height = CONTENT_BOX['full']
            # top: começa mais acima (bottom + 0.30), height 0.50 (reduzido para abrir espaço)
            ax_top = fig.add_axes([left, bottom + 0.30, width, 0.50])
            # hist: ocupar a faixa inferior (bastante espaço para labels)
            ax_hist = fig.add_axes([left, bottom + 0.04, width, 0.22])

            # BARRAS HORIZONTAIS (ticket médio)
            ax_top.barh(y_pos, vals, align='center', color='#2E86C1')
            ax_top.invert_yaxis()
            ax_top.set_yticks(y_pos)
            ax_top.set_yticklabels(names, fontsize=8)
            # Remover xlabel redundante do gráfico superior para evitar encavalamento
            ax_top.set_xlabel("")
            ax_top.xaxis.set_major_formatter(FuncFormatter(currency_fmt))
            if len(vals) > 0:
                ax_top.set_xlim(0, max(vals) * 1.12)
            _annotate_barh(ax_top, y_pos, vals, fmt_func=currency_fmt, inside_threshold=0.12)
            ax_top.set_title(f"Top {top_n_products} - Ticket Médio por Produto")

            # HISTOGRAMA (distribuição) - com espaço suficiente para ticks e título
            ax_hist.hist(resumo['valor_total'].dropna(), bins=30, color='#6AB04C')
            ax_hist.set_title("Distribuição de valor por transação", fontsize=10)
            ax_hist.set_xlabel("Valor total (R$)")
            ax_hist.xaxis.set_major_formatter(FuncFormatter(currency_fmt))
            ax_hist.tick_params(axis='x', rotation=0)

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1


        # --- PAGE 4: Vendas por categoria (padronizado) ---
        if not vendas_por_categoria.empty:
            vc = vendas_por_categoria.sort_values('valor_total', ascending=False)

            fig = plt.figure(figsize=FIGSIZE)
            fig.subplots_adjust(**MARGINS)
            _new_page(fig, title="Vendas por Categoria")

            left, bottom, width, height = CONTENT_BOX['full']
            ax1 = fig.add_axes([left, bottom+0.05, width*0.65, 0.78])
            ax2 = fig.add_axes([left+width*0.67, bottom+0.15, width*0.31, 0.65])

            ax1.bar(vc['categoria'].astype(str), vc['valor_total'], color='#2E86C1')
            ax1.set_xticklabels(vc['categoria'].astype(str), rotation=35, ha='right', fontsize=9)
            ax1.set_ylabel("Valor (R$)")
            ax1.yaxis.set_major_formatter(FuncFormatter(currency_fmt))

            ax2.pie(vc['valor_total'], labels=vc['categoria'], autopct='%1.1f%%', startangle=90, textprops={'fontsize':8})
            ax2.set_title("Participação (%)", fontsize=10)

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
                series = tmp.set_index('data').resample('M')['valor_total'].sum()
                fig = plt.figure(figsize=FIGSIZE)
                fig.subplots_adjust(**MARGINS)
                _new_page(fig, title="Vendas por Mês")

                left, bottom, width, height = CONTENT_BOX['full']
                ax = fig.add_axes([left, bottom+0.12, width, 0.7])
                ax.plot(series.index, series.values, marker='o', linewidth=2, color='#2E86C1')
                ax.set_xlabel("Mês")
                ax.set_ylabel("Valor total (R$)")
                ax.yaxis.set_major_formatter(FuncFormatter(currency_fmt))
                ax.tick_params(axis='x', rotation=45)

                _page_footer(fig, page)
                pdf.savefig(fig)
                plt.close(fig)
                page += 1

        # --- PAGE 6: Qualidade dos dados (texto formatado e sem sobreposição) ---
        fig = plt.figure(figsize=FIGSIZE)
        fig.subplots_adjust(**MARGINS)
        _new_page(fig, title="Qualidade dos Dados - Resumo")
        left, bottom, width, height = CONTENT_BOX['full']
        ax_text = fig.add_axes([left+0.01, bottom+0.02, width-0.02, height-0.15])
        ax_text.axis('off')

        lines = []
        if quality:
            for k,v in quality.items():
                lines.append(f"{k}: {v:,}" if isinstance(v, int) else f"{k}: {v}")
        else:
            if not resumo.empty:
                missing = {c: int(resumo[c].isna().sum()) for c in resumo.columns}
                dup_v = int(resumo.duplicated().sum())
                lines.append(f"Total registros no resumo: {len(resumo):,}")
                lines.append(f"Registros duplicados (resumo): {dup_v:,}")
                lines.append("Exemplo missing counts (col: n_missing):")
                nonzero = [(c,n) for c,n in missing.items() if n>0]
                if nonzero:
                    for c,n in nonzero[:12]:
                        lines.append(f" - {c}: {n:,}")
                else:
                    for c,n in list(missing.items())[:12]:
                        lines.append(f" - {c}: {n:,}")
        ax_text.text(0, 1, "\n".join(lines), fontsize=TEXT_FONT, family='monospace', va='top')
        _page_footer(fig, page)
        pdf.savefig(fig)
        plt.close(fig)
        page += 1

        # --- PAGE 7: Amostra registros (tabela) ---
        if not resumo.empty:
            sample = resumo.head(12).copy()
            cols_priority = ['id_venda','data','id_produto','id_empregado','quantidade','valor_unitario','valor_total','nome_emp','nome_prod','categoria']
            cols = [c for c in cols_priority if c in sample.columns]
            sample = sample[cols]
            for col in ['valor_unitario','valor_total','quantidade']:
                if col in sample.columns:
                    sample[col] = sample[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
            if 'data' in sample.columns:
                sample['data'] = pd.to_datetime(sample['data'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')

            fig = plt.figure(figsize=FIGSIZE)
            fig.subplots_adjust(**MARGINS)
            _new_page(fig, title="Amostra de registros (resumo)")
            left, bottom, width, height = CONTENT_BOX['full']
            ax_table = fig.add_axes([left, bottom, width, height])
            ax_table.axis('off')
            table = ax_table.table(cellText=sample.values, colLabels=sample.columns, cellLoc='left', loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(TABLE_FONT)
            table.scale(1, 1.1)

            _page_footer(fig, page)
            pdf.savefig(fig)
            plt.close(fig)
            page += 1

    print(f"PDF gerado: {output_pdf_path}")
