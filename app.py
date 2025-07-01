import streamlit as st
import pandas as pd
import os
import json

st.set_page_config(layout="wide")

# ==== Autenticação simples ====
senha_correta = "1234"
if "autenticado" not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    senha = st.text_input("Digite a senha de acesso:", type="password")
    if senha == senha_correta:
        st.session_state.autenticado = True
        st.success("✅ Acesso autorizado.")
    else:
        if senha != "":
            st.error("❌ Senha incorreta.")
        st.stop()

# ==== Navegação ====
st.sidebar.title("🔍 Navegação")
menu = st.sidebar.radio("Ir para:", ["Consulta Individual", "Registros de Consulta Ativa", "Resumo", "Atualizar Bases"])

# ==== Carregamento das bases ====
if "df" not in st.session_state or "tomb" not in st.session_state:
    if os.path.exists("NovoEmprestimo.xlsx") and os.path.exists("Tombamento.xlsx"):
        df = pd.read_excel("NovoEmprestimo.xlsx")
        tomb = pd.read_excel("Tombamento.xlsx")
    else:
        st.sidebar.warning("📂 Carregue as bases para iniciar.")
        novo_file = st.sidebar.file_uploader("NovoEmprestimo.xlsx", type="xlsx")
        tomb_file = st.sidebar.file_uploader("Tombamento.xlsx", type="xlsx")
        if novo_file and tomb_file:
            with open("NovoEmprestimo.xlsx", "wb") as f:
                f.write(novo_file.getbuffer())
            with open("Tombamento.xlsx", "wb") as f:
                f.write(tomb_file.getbuffer())
            df = pd.read_excel("NovoEmprestimo.xlsx")
            tomb = pd.read_excel("Tombamento.xlsx")
        else:
            st.stop()

    df["Número CPF/CNPJ"] = df["Número CPF/CNPJ"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
    df["Número Contrato Crédito"] = df["Número Contrato Crédito"].astype(str)
    tomb["CPF Tomador"] = tomb["CPF Tomador"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
    tomb["Número Contrato"] = tomb["Número Contrato"].astype(str)

    st.session_state.df = df
    st.session_state.tomb = tomb

# ==== Persistência de CPFs marcados como Consulta Ativa ====
CPFS_ATIVOS_FILE = "consulta_ativa.json"
if "cpfs_ativos" not in st.session_state:
    if os.path.exists(CPFS_ATIVOS_FILE):
        with open(CPFS_ATIVOS_FILE, "r") as f:
            st.session_state.cpfs_ativos = json.load(f)
    else:
        st.session_state.cpfs_ativos = []

# ==== Consulta Individual ====
if menu == "Consulta Individual":
    st.title("🔎 Consulta Individual")
    df = st.session_state.df
    tomb = st.session_state.tomb

    cpf_input = st.text_input("Digite o CPF (somente números):", max_chars=11)
    if cpf_input and len(cpf_input) == 11:
        if st.button("Consultar"):
            df_filtrado = df[
                (df["Número CPF/CNPJ"] == cpf_input) &
                (df["Submodalidade Bacen"] == "CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.") &
                (df["Critério Débito"] == "FOLHA DE PAGAMENTO") &
                (~df["Código Linha Crédito"].isin([140073, 138358, 141011]))
            ]
            if not df_filtrado.empty:
                resultado = df_filtrado.merge(
                    tomb,
                    left_on=["Número CPF/CNPJ", "Número Contrato Crédito"],
                    right_on=["CPF Tomador", "Número Contrato"],
                    how="left"
                )
                resultado["CNPJ Empresa Consignante"] = resultado["CNPJ Empresa Consignante"].fillna("CONSULTE SISBR")
                resultado["Empresa Consignante"] = resultado["Empresa Consignante"].fillna("CONSULTE SISBR")
                st.dataframe(resultado[[
                    "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito", "Quantidade Parcelas Abertas",
                    "% Taxa Operação", "Código Linha Crédito", "Nome Comercial",
                    "CNPJ Empresa Consignante", "Empresa Consignante"
                ]])
                if cpf_input not in st.session_state.cpfs_ativos:
                    if st.button("Marcar como Consulta Ativa"):
                        st.session_state.cpfs_ativos.append(cpf_input)
                        with open(CPFS_ATIVOS_FILE, "w") as f:
                            json.dump(st.session_state.cpfs_ativos, f)
                        st.success("✅ CPF marcado com sucesso.")
            else:
                st.warning("Nenhum contrato encontrado com os critérios informados.")

# ==== Registros de Consulta Ativa ====
elif menu == "Registros de Consulta Ativa":
    st.title("📌 Registros com Consulta Ativa")
    df = st.session_state.df
    tomb = st.session_state.tomb
    ativos = st.session_state.cpfs_ativos
    if not ativos:
        st.info("Nenhum CPF marcado como Consulta Ativa.")
    else:
        ativos_df = df[df["Número CPF/CNPJ"].isin(ativos)].copy()
        ativos_df = ativos_df.merge(
            tomb,
            left_on=["Número CPF/CNPJ", "Número Contrato Crédito"],
            right_on=["CPF Tomador", "Número Contrato"],
            how="left"
        )
        ativos_df["CNPJ Empresa Consignante"] = ativos_df["CNPJ Empresa Consignante"].fillna("CONSULTE SISBR")
        ativos_df["Empresa Consignante"] = ativos_df["Empresa Consignante"].fillna("CONSULTE SISBR")
        st.dataframe(ativos_df[[
            "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito",
            "CNPJ Empresa Consignante", "Empresa Consignante"
        ]])

# ==== Resumo ====
elif menu == "Resumo":
    st.title("📊 Resumo Consolidado por Consignante")
    df = st.session_state.df
    tomb = st.session_state.tomb
    ativos = st.session_state.cpfs_ativos

    base = df.merge(
        tomb,
        left_on=["Número CPF/CNPJ", "Número Contrato Crédito"],
        right_on=["CPF Tomador", "Número Contrato"],
        how="left"
    )
    base["CNPJ Empresa Consignante"] = base["CNPJ Empresa Consignante"].fillna("CONSULTE SISBR")
    base["Empresa Consignante"] = base["Empresa Consignante"].fillna("CONSULTE SISBR")
    base["Consulta Ativa"] = base["Número CPF/CNPJ"].isin(ativos)

    resumo = base.groupby(["CNPJ Empresa Consignante", "Empresa Consignante"]).agg(
        Total_Cooperados=("Número CPF/CNPJ", "nunique"),
        Total_Contratos=("Número Contrato Crédito", "count"),
        Total_Consulta_Ativa=("Consulta Ativa", "sum")
    ).reset_index()
    st.dataframe(resumo)

    st.markdown("### 📥 Exportar Relação Analítica")
    base["Consulta Ativa"] = base["Consulta Ativa"].apply(lambda x: "Sim" if x else "Não")
    analitico = base[[
        "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito", "Empresa Consignante",
        "CNPJ Empresa Consignante", "Consulta Ativa"
    ]]
    csv = analitico.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Baixar Analítico CSV", csv, "relacao_analitica.csv", "text/csv")

# ==== Atualizar Bases ====
elif menu == "Atualizar Bases":
    st.title("🔄 Atualizar Bases de Dados")
    novo_file = st.file_uploader("📄 NovoEmprestimo.xlsx", type="xlsx")
    tomb_file = st.file_uploader("📄 Tombamento.xlsx", type="xlsx")
    if novo_file and tomb_file:
        with open("NovoEmprestimo.xlsx", "wb") as f:
            f.write(novo_file.getbuffer())
        with open("Tombamento.xlsx", "wb") as f:
            f.write(tomb_file.getbuffer())
        st.success("✅ Bases atualizadas. Recarregue a página.")


