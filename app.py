
import streamlit as st
import pandas as pd
import os
import json

st.set_page_config(layout="wide")

# Autenticação simples por senha
senha_correta = "1234"
if "autenticado" not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    senha = st.text_input("Digite a senha de acesso:", type="password")
    if senha != senha_correta:
        st.stop()
    if senha == senha_correta:
        st.session_state.autenticado = True
        st.success("✅ Acesso liberado.")
    else:
        st.stop()

st.sidebar.title("🔍 Navegação")
menu = st.sidebar.radio("Ir para:", ["Consulta Individual", "Registros de Consulta Ativa", "Resumo", "Atualizar Bases"])


# Verificar se as bases já foram salvas localmente
if os.path.exists("NovoEmprestimo.xlsx") and os.path.exists("Tombamento.xlsx"):
    df = pd.read_excel("NovoEmprestimo.xlsx")
    tomb = pd.read_excel("Tombamento.xlsx")
    st.session_state.df = df
    st.session_state.tomb = tomb
else:
    st.sidebar.warning("📂 Carregue as bases de dados para iniciar.")
    novo_file = st.sidebar.file_uploader("📄 NovoEmprestimo.xlsx", type="xlsx")
    tomb_file = st.sidebar.file_uploader("📄 Tombamento.xlsx", type="xlsx")
    if novo_file and tomb_file:
        with open("NovoEmprestimo.xlsx", "wb") as f:
            f.write(novo_file.getbuffer())
        with open("Tombamento.xlsx", "wb") as f:
            f.write(tomb_file.getbuffer())

        df = pd.read_excel("NovoEmprestimo.xlsx")
        tomb = pd.read_excel("Tombamento.xlsx")

        df["Número CPF/CNPJ"] = df["Número CPF/CNPJ"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
        df["Número Contrato Crédito"] = df["Número Contrato Crédito"].astype(str)
        tomb["CPF Tomador"] = tomb["CPF Tomador"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
        tomb["Número Contrato"] = tomb["Número Contrato"].astype(str)

        st.session_state.df = df
        st.session_state.tomb = tomb
        st.success("✅ Bases carregadas com sucesso.")
# 📌 Persistência dos CPFs marcados como 'Consulta Ativa'
CPFS_ATIVOS_FILE = "consulta_ativa.json"
if "cpfs_ativos" not in st.session_state:
    if os.path.exists(CPFS_ATIVOS_FILE):
        with open(CPFS_ATIVOS_FILE, "r") as f:
            st.session_state.cpfs_ativos = json.load(f)
    else:
        st.session_state.cpfs_ativos = []
        st.warning("Nenhum contrato encontrado com os critérios informados.")

if menu == "Registros de Consulta Ativa":
    st.title("📌 Registros com Consulta Ativa")
    df_filtrado = df[
        (df["Submodalidade Bacen"] == "CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.") &
        (df["Critério Débito"] == "FOLHA DE PAGAMENTO") &
        (~df["Código Linha Crédito"].isin([140073, 138358, 141011]))
    ]
    df_filtrado["Número Contrato Crédito"] = df_filtrado["Número Contrato Crédito"].astype(str)
    tomb["Número Contrato"] = tomb["Número Contrato"].astype(str)

    merged = pd.merge(df_filtrado, tomb,
                      left_on=["Número CPF/CNPJ", "Número Contrato Crédito"],
                      right_on=["CPF Tomador", "Número Contrato"], how="left")
    merged = merged[merged["Número CPF/CNPJ"].isin(cpfs_ativos)]

    if not merged.empty:
        st.dataframe(merged[[
            "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito",
            "Quantidade Parcelas Abertas", "% Taxa Operação", "Código Linha Crédito",
            "Nome Comercial", "CNPJ Empresa Consignante", "Empresa Consignante"
        ]])
    else:
        st.warning("Nenhum registro com Consulta Ativa.")

if menu == "Resumo":
    st.title("📊 Resumo por Consignante")

    df_filtrado = df[
        (df["Submodalidade Bacen"] == "CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.") &
        (df["Critério Débito"] == "FOLHA DE PAGAMENTO") &
        (~df["Código Linha Crédito"].isin([140073, 138358, 141011]))
    ]

    df_filtrado["Número Contrato Crédito"] = df_filtrado["Número Contrato Crédito"].astype(str)
    tomb["Número Contrato"] = tomb["Número Contrato"].astype(str)

    merged = pd.merge(df_filtrado, tomb,
                      left_on=["Número CPF/CNPJ", "Número Contrato Crédito"],
                      right_on=["CPF Tomador", "Número Contrato"], how="left")

    merged["Consulta Ativa"] = merged["Número CPF/CNPJ"].isin(cpfs_ativos)
    merged["Consulta Ativa"] = merged["Consulta Ativa"].apply(lambda x: "Sim" if x else "Não")

    resumo = merged.groupby(["CNPJ Empresa Consignante", "Empresa Consignante"]).agg(
        Total_Cooperados=("Número CPF/CNPJ", "nunique"),
        Total_de_Contratos=("Número Contrato Crédito", "count"),
        Total_Consulta_Ativa=("Consulta Ativa", lambda x: (x == "Sim").sum())
    ).reset_index()

    st.dataframe(resumo)

    st.markdown("### 📥 Exportar Relação Analítica")
    analitico = merged[[
        "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito",
        "Quantidade Parcelas Abertas", "% Taxa Operação", "Código Linha Crédito",
        "Nome Comercial", "CNPJ Empresa Consignante", "Empresa Consignante",
        "Consulta Ativa"
    ]]
    csv = analitico.to_csv(index=False).encode("utf-8")
    st.download_button("📤 Baixar CSV Analítico", data=csv, file_name="relacao_analitica.csv", mime="text/csv")

if menu == "Atualizar Bases":
    st.title("🔄 Atualizar Bases de Dados")
    novo_file = st.file_uploader("📄 NovoEmprestimo.xlsx", type="xlsx")
    tomb_file = st.file_uploader("📄 Tombamento.xlsx", type="xlsx")

    if novo_file and tomb_file:
        df = pd.read_excel(novo_file)
        tomb = pd.read_excel(tomb_file)

        df["Número CPF/CNPJ"] = df["Número CPF/CNPJ"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
        df["Número Contrato Crédito"] = df["Número Contrato Crédito"].astype(str)
        tomb["CPF Tomador"] = tomb["CPF Tomador"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
        tomb["Número Contrato"] = tomb["Número Contrato"].astype(str)

        st.session_state.df = df
        st.session_state.tomb = tomb
        st.success("✅ Bases atualizadas com sucesso.")





