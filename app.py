
import streamlit as st
import pandas as pd
import os
import json
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(page_title="Consulta de Empréstimos", layout="wide")

# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(st.secrets["gspread"]["json"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open("consulta_ativa").sheet1

def carregar_cpfs_ativos():
    try:
        values = sheet.get_all_values()
        if not values or len(values) < 2:
            return []
        return [row[0] for row in values[1:]]  # Ignora cabeçalho
    except:
        return []

def marcar_cpf_ativo(cpf):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.append_row([cpf, timestamp])

cpfs_ativos = carregar_cpfs_ativos()

# Inicialização do estado
for key in ["autenticado", "arquivo_novo", "arquivo_tomb"]:
    if key not in st.session_state:
        st.session_state[key] = None if key != "autenticado" else False

DATA_DIR = "data"
NOVO_PATH = os.path.join(DATA_DIR, "novoemprestimo.xlsx")
TOMB_PATH = os.path.join(DATA_DIR, "tombamento.xlsx")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def autenticar():
    senha = st.text_input("Digite a senha para acessar o sistema:", type="password")
    if senha == "tombamento":
        st.session_state.autenticado = True
        st.success("Acesso autorizado.")
    elif senha:
        st.error("Senha incorreta.")

autenticar()
if not st.session_state.autenticado:
    st.stop()

def formatar_documentos(df, col, tamanho):
    return df[col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(tamanho)

def carregar_bases_do_disco():
    st.session_state.novo_df = pd.read_excel(NOVO_PATH)
    st.session_state.tomb_df = pd.read_excel(TOMB_PATH)

    st.session_state.novo_df['Número CPF/CNPJ'] = formatar_documentos(st.session_state.novo_df, 'Número CPF/CNPJ', 11)
    st.session_state.tomb_df['CPF Tomador'] = formatar_documentos(st.session_state.tomb_df, 'CPF Tomador', 11)
    if 'Número Contrato' in st.session_state.tomb_df.columns:
        st.session_state.tomb_df['Número Contrato'] = st.session_state.tomb_df['Número Contrato'].astype(str)

def salvar_arquivos(upload_novo, upload_tomb):
    with open(NOVO_PATH, "wb") as f:
        f.write(upload_novo.read())
    with open(TOMB_PATH, "wb") as f:
        f.write(upload_tomb.read())
    carregar_bases_do_disco()

st.sidebar.header("Menu")
menu = st.sidebar.radio("Navegação", ["Consulta Individual", "Registros Consulta Ativa", "Resumo", "Inconsistências", "Atualizar Bases"])

if menu == "Atualizar Bases":
    st.session_state.arquivo_novo = st.sidebar.file_uploader("Nova Base NovoEmprestimo.xlsx", type="xlsx")
    st.session_state.arquivo_tomb = st.sidebar.file_uploader("Nova Base Tombamento.xlsx", type="xlsx")
    if st.sidebar.button("Atualizar"):
        if st.session_state.arquivo_novo and st.session_state.arquivo_tomb:
            salvar_arquivos(st.session_state.arquivo_novo, st.session_state.arquivo_tomb)
            st.success("Bases atualizadas.")
            st.rerun()
        else:
            st.warning("Envie os dois arquivos para atualizar.")
    st.stop()

if not os.path.exists(NOVO_PATH) or not os.path.exists(TOMB_PATH):
    st.info("Faça o upload das bases para iniciar o sistema.")
    arquivo_novo = st.file_uploader("Base NovoEmprestimo.xlsx", type="xlsx", key="upload_novo")
    arquivo_tomb = st.file_uploader("Base Tombamento.xlsx", type="xlsx", key="upload_tomb")
    if arquivo_novo and arquivo_tomb:
        salvar_arquivos(arquivo_novo, arquivo_tomb)
        st.success("Bases carregadas com sucesso.")
        st.rerun()
    else:
        st.stop()
else:
    carregar_bases_do_disco()

if menu == "Consulta Individual":
    st.title("🔍 Consulta de Empréstimos por CPF")
    cpf_input = st.text_input("Digite o CPF (apenas números):").strip()

    if cpf_input and len(cpf_input) == 11 and cpf_input.isdigit():
        df = st.session_state.novo_df
        tomb = st.session_state.tomb_df

        filtrado = df[
            (df['Número CPF/CNPJ'] == cpf_input) &
            (df['Submodalidade Bacen'] == 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.') &
            (df['Critério Débito'] == 'FOLHA DE PAGAMENTO') &
            (~df['Código Linha Crédito'].isin([140073, 138358, 141011, 101014, 137510]))
        ]

        if filtrado.empty:
            st.warning("Nenhum contrato encontrado com os filtros aplicados.")
        else:
            resultados = []
            for _, row in filtrado.iterrows():
                contrato = str(row['Número Contrato Crédito'])
                match = tomb[
                    (tomb['CPF Tomador'] == cpf_input) &
                    (tomb['Número Contrato'] == contrato)
                ]

                consignante = match['CNPJ Empresa Consignante'].iloc[0] if not match.empty else "CONSULTE SISBR"
                empresa = match['Empresa Consignante'].iloc[0] if not match.empty else "CONSULTE SISBR"

                resultados.append({
                    "Número CPF/CNPJ": row['Número CPF/CNPJ'],
                    "Nome Cliente": row['Nome Cliente'],
                    "Número Contrato Crédito": contrato,
                    "Quantidade Parcelas Abertas": row['Quantidade Parcelas Abertas'],
                    "% Taxa Operação": row['% Taxa Operação'],
                    "Código Linha Crédito": row['Código Linha Crédito'],
                    "Nome Comercial": row['Nome Comercial'],
                    "Consignante": consignante,
                    "Empresa Consignante": empresa
                })

            st.dataframe(pd.DataFrame(resultados))
            if cpf_input in cpfs_ativos:
                st.info("✅ CPF já marcado como Consulta Ativa.")
            else:
                if st.button("Marcar como Consulta Ativa"):
                    marcar_cpf_ativo(cpf_input)
                    st.success("✅ CPF marcado com sucesso.")
                    st.rerun()

if menu == "Registros Consulta Ativa":
    st.title("📋 Registros de Consulta Ativa")

    df = st.session_state.novo_df
    tomb = st.session_state.tomb_df

    registros = []

    for cpf_input in cpfs_ativos:
        filtrado = df[
            (df['Número CPF/CNPJ'] == cpf_input) &
            (df['Submodalidade Bacen'] == 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.') &
            (df['Critério Débito'] == 'FOLHA DE PAGAMENTO') &
            (~df['Código Linha Crédito'].isin([140073, 138358, 141011]))
        ]

        for _, row in filtrado.iterrows():
            contrato = str(row['Número Contrato Crédito'])
            match = tomb[
                (tomb['CPF Tomador'] == cpf_input) &
                (tomb['Número Contrato'] == contrato)
            ]

            consignante = match['CNPJ Empresa Consignante'].iloc[0] if not match.empty else "CONSULTE SISBR"
            empresa = match['Empresa Consignante'].iloc[0] if not match.empty else "CONSULTE SISBR"

            registros.append({
                "Número CPF/CNPJ": row['Número CPF/CNPJ'],
                "Nome Cliente": row['Nome Cliente'],
                "Número Contrato Crédito": contrato,
                "Quantidade Parcelas Abertas": row['Quantidade Parcelas Abertas'],
                "% Taxa Operação": row['% Taxa Operação'],
                "Código Linha Crédito": row['Código Linha Crédito'],
                "Nome Comercial": row['Nome Comercial'],
                "Consignante": consignante,
                "Empresa Consignante": empresa
            })

    if registros:
        st.dataframe(pd.DataFrame(registros))
    else:
        st.info("Nenhum registro encontrado para os CPFs marcados como Consulta Ativa.")


if menu == "Resumo":
    st.title("📊 Resumo Consolidado por Consignante (Base Completa)")

    df = st.session_state.novo_df
    tomb = st.session_state.tomb_df

    registros = []

    for _, row in df.iterrows():
        cpf = row['Número CPF/CNPJ']
        contrato = str(row['Número Contrato Crédito'])

        if row['Submodalidade Bacen'] != 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.':
            continue
        if row['Critério Débito'] != 'FOLHA DE PAGAMENTO':
            continue
        if row['Código Linha Crédito'] in [140073, 138358, 141011]:
            continue

        match = tomb[
            (tomb['CPF Tomador'] == cpf) &
            (tomb['Número Contrato'] == contrato)
        ]

        consignante = match['CNPJ Empresa Consignante'].iloc[0] if not match.empty else "CONSULTE SISBR"
        empresa = match['Empresa Consignante'].iloc[0] if not match.empty else "CONSULTE SISBR"

        registros.append({
            "CNPJ Empresa Consignante": consignante,
            "Empresa Consignante": empresa,
            "CPF": cpf,
            "Consulta Ativa": cpf in cpfs_ativos
        })

    if registros:
        df_registros = pd.DataFrame(registros)

        resumo = df_registros.groupby(["CNPJ Empresa Consignante", "Empresa Consignante"]).agg(
            Total_Cooperados=("CPF", "nunique"),
            Total_Contratos=("CPF", "count"),
            Total_Consulta_Ativa=("Consulta Ativa", "sum")
        ).reset_index()

        st.dataframe(resumo)

        with st.expander("📥 Exportar relação analítica"):
            import io
            with io.BytesIO() as buffer:
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_registros.to_excel(writer, index=False, sheet_name="Relação Analítica")
                buffer.seek(0)
                st.download_button(
                    label="Exportar para Excel",
                    data=buffer,
                    file_name="resumo_analitico.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    else:
        st.info("Nenhum dado encontrado na base para resumo.")


if menu == "Inconsistências":
    st.title("🚨 Contratos sem Correspondência no Tombamento")

    df = st.session_state.novo_df
    tomb = st.session_state.tomb_df

    df['Número CPF/CNPJ'] = df['Número CPF/CNPJ'].astype(str).str.replace(r'\D', '', regex=True).str.zfill(11)
    tomb['CPF Tomador'] = tomb['CPF Tomador'].astype(str).str.replace(r'\D', '', regex=True).str.zfill(11)
    tomb['Número Contrato'] = tomb['Número Contrato'].astype(str)

    filtrado = df[
        (df['Submodalidade Bacen'] == 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.') &
        (df['Critério Débito'] == 'FOLHA DE PAGAMENTO') &
        (~df['Código Linha Crédito'].isin([140073, 138358, 141011]))
    ].copy()

    filtrado['Origem'] = filtrado.apply(
        lambda row: "TOMBAMENTO" if not tomb[
            (tomb['CPF Tomador'] == row['Número CPF/CNPJ']) &
            (tomb['Número Contrato'] == str(row['Número Contrato Crédito']))
        ].empty else "CONSULTE SISBR", axis=1
    )

    inconsistencias = filtrado[filtrado['Origem'] == 'CONSULTE SISBR'][
        ['Número CPF/CNPJ', 'Número Contrato Crédito', 'Código Linha Crédito', 'Nome Cliente']
    ]

    if inconsistencias.empty:
        st.success("Nenhuma inconsistência encontrada.")
    else:
        st.warning(f"{len(inconsistencias)} contratos sem correspondência no tombamento encontrados.")
        st.dataframe(inconsistencias)

        with st.expander("📥 Exportar inconsistências"):
            import io
            with io.BytesIO() as buffer:
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    inconsistencias.to_excel(writer, index=False, sheet_name="Inconsistencias")
                buffer.seek(0)
                st.download_button(
                    label="Exportar para Excel",
                    data=buffer,
                    file_name="inconsistencias_tombamento.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

