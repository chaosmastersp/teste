
import streamlit as st
import pandas as pd
import os
import json

st.set_page_config(page_title="Consulta de Empréstimos", layout="wide")

DATA_DIR = "data"
NOVO_PATH = os.path.join(DATA_DIR, "novoemprestimo.xlsx")
TOMB_PATH = os.path.join(DATA_DIR, "tombamento.xlsx")
CONSULTA_ATIVA_PATH = os.path.join(DATA_DIR, "consultas_ativas.json")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

for key in ["autenticado", "arquivo_novo", "arquivo_tomb"]:
    if key not in st.session_state:
        st.session_state[key] = None if key != "autenticado" else False

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

def carregar_cpfs_ativos():
    if os.path.exists(CONSULTA_ATIVA_PATH):
        with open(CONSULTA_ATIVA_PATH, "r") as f:
            return json.load(f).get("cpfs", [])
    return []

def salvar_cpfs_ativos(lista):
    with open(CONSULTA_ATIVA_PATH, "w") as f:
        json.dump({"cpfs": lista}, f)

menu = st.sidebar.radio("📌 Navegação", ["Consulta Individual", "Registros de Consulta Ativa", "Resumo", "Atualizar Bases"])

if menu == "Atualizar Bases":
    st.sidebar.markdown("### Upload de Novas Bases")
    st.session_state.arquivo_novo = st.sidebar.file_uploader("Nova Base NovoEmprestimo.xlsx", type="xlsx")
    st.session_state.arquivo_tomb = st.sidebar.file_uploader("Nova Base Tombamento.xlsx", type="xlsx")
    if st.session_state.arquivo_novo and st.session_state.arquivo_tomb:
        salvar_arquivos(st.session_state.arquivo_novo, st.session_state.arquivo_tomb)
        st.rerun()
    else:
        st.warning("⚠️ Envie os dois arquivos para atualizar.")
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

df = st.session_state.novo_df
tomb = st.session_state.tomb_df
cpfs_ativos = carregar_cpfs_ativos()

if menu == "Resumo":
    st.title("📊 Resumo por Empresa Consignante")

    df_filtrado = df[
        (df['Submodalidade Bacen'] == 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.') &
        (df['Critério Débito'] == 'FOLHA DE PAGAMENTO') &
        (~df['Código Linha Crédito'].isin([140073, 138358, 141011]))
    ]
    df_filtrado['Número Contrato Crédito'] = df_filtrado['Número Contrato Crédito'].astype(str)
tomb['Número Contrato'] = tomb['Número Contrato'].astype(str)
merged = pd.merge(df_filtrado, tomb, left_on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
                      right_on=['CPF Tomador', 'Número Contrato'], how='left')

merged['Consulta Ativa'] = merged['Número CPF/CNPJ'].isin(cpfs_ativos)

resumo = merged.groupby(['CNPJ Empresa Consignante', 'Empresa Consignante']).agg(
        Total_Cooperados=('Número CPF/CNPJ', 'nunique'),
        Total_de_Contratos=('Número Contrato Crédito', 'count'),
        Total_Consulta_Ativa=('Consulta Ativa', 'sum')
    ).reset_index()

st.dataframe(resumo)

    # Exportar relação analítica
    st.markdown("### 📥 Exportar Relação Analítica")
    merged['Consulta Ativa'] = merged['Consulta Ativa'].apply(lambda x: 'Sim' if x else 'Não')
    analitico = merged[[
        'Número CPF/CNPJ', 'Nome Cliente', 'Número Contrato Crédito', 'Quantidade Parcelas Abertas',
        '% Taxa Operação', 'Código Linha Crédito', 'Nome Comercial',
        'CNPJ Empresa Consignante', 'Empresa Consignante', 'Consulta Ativa'
    ]]
    csv = analitico.to_csv(index=False).encode('utf-8')
    st.download_button("📤 Baixar relação analítica (.csv)", data=csv, file_name="relacao_analitica.csv", mime="text/csv")


