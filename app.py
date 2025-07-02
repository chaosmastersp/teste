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


def carregar_tombados_google():
    try:
        tomb_sheet = client.open("consulta_ativa").worksheet("tombados")
        values = tomb_sheet.get_all_values()
        if not values or len(values) < 2:
            return set()
        return set((row[0], row[1]) for row in values[1:])  # (cpf, contrato)
    except:
        return set()

def marcar_tombado(cpf, contrato):
    try:
        tomb_sheet = client.open("consulta_ativa").worksheet("tombados")
    except:
        tomb_sheet = client.open("consulta_ativa").add_worksheet(title="tombados", rows="1000", cols="3")
        tomb_sheet.append_row(["cpf", "contrato", "timestamp"])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tomb_sheet.append_row([cpf, contrato, timestamp])


def marcar_cpf_ativo(cpf):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.append_row([cpf, timestamp])


def carregar_aguardando_google():
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
        values = aguard_sheet.get_all_values()
        if not values or len(values) < 2:
            return set()
        return set((row[0], row[1]) for row in values[1:])
    except:
        return set()

def marcar_aguardando(cpf, contrato):
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
    except:
        aguard_sheet = client.open("consulta_ativa").add_worksheet(title="aguardando", rows="1000", cols="3")
        aguard_sheet.append_row(["cpf", "contrato", "timestamp"])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    aguard_sheet.append_row([cpf, contrato, timestamp])


cpfs_ativos = carregar_cpfs_ativos()
tombados = carregar_tombados_google()
aguardando = carregar_aguardando_google()

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
menu = st.sidebar.radio("Navegação", [
    "Consulta Individual",
    "Registros Consulta Ativa",
    "Aguardando Conclusão",
    "Tombado",
    "Resumo",
    "Inconsistências",
    "Atualizar Bases"
])

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
    cpf_input = st.text_input("Digite o CPF (apenas números):", key="cpf_consulta").strip()

    if "ultimo_cpf_consultado" not in st.session_state:
        st.session_state.ultimo_cpf_consultado = None

    if st.button("Consultar"):
        st.session_state.ultimo_cpf_consultado = cpf_input

    if st.session_state.ultimo_cpf_consultado:
        cpf_validado = st.session_state.ultimo_cpf_consultado

        if cpf_validado and len(cpf_validado) == 11 and cpf_validado.isdigit():
            df = st.session_state.novo_df
    total_tb = len(tombados)
            tomb = st.session_state.tomb_df

            filtrado = df[
                (df['Número CPF/CNPJ'] == cpf_validado) &
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
                        (tomb['CPF Tomador'] == cpf_validado) &
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
                if cpf_validado in cpfs_ativos:
                    st.info("✅ CPF já marcado como Consulta Ativa.")
                else:
                    if st.button("Marcar como Consulta Ativa"):
                        marcar_cpf_ativo(cpf_validado)
                        st.success("✅ CPF marcado com sucesso.")
                        st.rerun()
        else:
            st.warning("CPF inválido. Digite exatamente 11 números.")


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
            (~df['Código Linha Crédito'].isin([140073, 138358, 141011, 101014, 137510]))
        ]

        for _, row in filtrado.iterrows():
            contrato = str(row['Número Contrato Crédito'])
            if (cpf_input, contrato) in tombados or (cpf_input, contrato) in aguardando:
                continue

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
        df_resultado = pd.DataFrame(registros)
        total_ag = len(df_resultado)
        st.dataframe(df_resultado, use_container_width=True)

        cpfs_disponiveis = df_resultado['Número CPF/CNPJ'].unique().tolist()
        cpf_escolhido = st.selectbox("Selecione o CPF para marcar como Lançado Sisbr:", cpfs_disponiveis)

        if st.button("Marcar todos os contratos como Lançado Sisbr"):
            contratos = df_resultado[df_resultado['Número CPF/CNPJ'] == cpf_escolhido]['Número Contrato Crédito'].astype(str).tolist()
            for contrato in contratos:
                marcar_aguardando(cpf_escolhido, contrato)
            st.success(f"Todos os contratos do CPF {cpf_escolhido} foram movidos para 'Aguardando Conclusão'.")
            st.rerun()
    else:
        st.info("Nenhum registro disponível para Consulta Ativa.")



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
        if row['Código Linha Crédito'] in [140073, 138358, 141011, 101014, 137510]:
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
            "Contrato": contrato,
            "Consulta Ativa": cpf in cpfs_ativos,
            "Tombado": (cpf, contrato) in tombados,
            "Aguardando": (cpf, contrato) in aguardando
        })

    if registros:
        df_registros = pd.DataFrame(registros)

        resumo = df_registros.groupby(["CNPJ Empresa Consignante", "Empresa Consignante"]).agg(
            Total_Cooperados=("CPF", "nunique"),
            Total_Contratos=("Contrato", "count"),
            Total_Consulta_Ativa=("Consulta Ativa", "sum"),
            Total_Tombado=("Tombado", "sum"),
            Total_Aguardando_Conclusao=("Aguardando", "sum")
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
        (~df['Código Linha Crédito'].isin([140073, 138358, 141011, 101014, 137510]))
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



if menu == "Aguardando Conclusão":
    st.title("⏳ Registros Aguardando Conclusão")

    total_ag = 0

    df = st.session_state.novo_df
    tomb = st.session_state.tomb_df

    registros = []

    for cpf_input, contrato in aguardando:
        match_df = df[
            (df['Número CPF/CNPJ'] == cpf_input) &
            (df['Número Contrato Crédito'].astype(str) == contrato)
        ]

        for _, row in match_df.iterrows():
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
        df_resultado = pd.DataFrame(registros)
        st.dataframe(df_resultado, use_container_width=True)

        cpfs_disponiveis = df_resultado['Número CPF/CNPJ'].unique().tolist()
        cpf_escolhido = st.selectbox("Selecione o CPF para tombar os contratos:", cpfs_disponiveis)

        if st.button("Marcar todos os contratos como Tombado"):
            contratos = df_resultado[df_resultado['Número CPF/CNPJ'] == cpf_escolhido]['Número Contrato Crédito'].astype(str).tolist()
            for contrato in contratos:
                marcar_tombado(cpf_escolhido, contrato)
            st.success(f"Todos os contratos do CPF {cpf_escolhido} foram tombados com sucesso.")
            st.rerun()
    else:
        st.info("Nenhum registro marcado como Lançado Sisbr encontrado.")


if menu == "Tombado":
    st.title("📁 Registros Tombados")

    total_tb = 0

    df = st.session_state.novo_df
    tomb = st.session_state.tomb_df

    registros = []

    for cpf_input, contrato in tombados:
        match_df = df[
            (df['Número CPF/CNPJ'] == cpf_input) &
            (df['Número Contrato Crédito'].astype(str) == contrato)
        ]
        for _, row in match_df.iterrows():
            tomb_match = tomb[
                (tomb['CPF Tomador'] == cpf_input) &
                (tomb['Número Contrato'] == contrato)
            ]
            consignante = tomb_match['CNPJ Empresa Consignante'].iloc[0] if not tomb_match.empty else "CONSULTE SISBR"
            empresa = tomb_match['Empresa Consignante'].iloc[0] if not tomb_match.empty else "CONSULTE SISBR"

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
        st.warning(f"{len(registros)} contratos marcados como Consulta Ativa encontrados.")
    st.dataframe(pd.DataFrame(registros))
    else:
        st.info("Nenhum contrato marcado como tombado encontrado.")
