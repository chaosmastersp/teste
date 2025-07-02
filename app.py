import streamlit as st
import pandas as pd
import os
import json
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(page_title="Consulta de Empréstimos", layout="wide")

# Google Sheets Setup - Use st.cache_resource for the gspread client
@st.cache_resource
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(st.secrets["gspread"]["json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

client = get_gspread_client()

@st.cache_data(ttl=300) # Cache for 5 minutes to keep data relatively fresh
def carregar_cpfs_ativos():
    try:
        sheet = client.open("consulta_ativa").sheet1
        values = sheet.get_all_values()
        if not values or len(values) < 2:
            return []
        return [row[0] for row in values[1:]]  # Ignora cabeçalho
    except Exception as e:
        st.error(f"Erro ao carregar CPFs ativos: {e}")
        return []

@st.cache_data(ttl=300)
def carregar_tombados_google():
    try:
        tomb_sheet = client.open("consulta_ativa").worksheet("tombados")
        values = tomb_sheet.get_all_values()
        if not values or len(values) < 2:
            return set()
        return set((row[0], row[1]) for row in values[1:])  # (cpf, contrato)
    except Exception as e:
        st.error(f"Erro ao carregar registros tombados: {e}")
        return set()

@st.cache_data(ttl=300)
def carregar_aguardando_google():
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
        values = aguard_sheet.get_all_values()
        if not values or len(values) < 2:
            return set()
        return set((row[0], row[1]) for row in values[1:])
    except Exception as e:
        st.error(f"Erro ao carregar registros aguardando: {e}")
        return set()

# Functions that modify Google Sheets should not be cached, but their calls should invalidate relevant caches
def marcar_tombado(cpf, contrato):
    consulta = client.open("consulta_ativa")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Adiciona à planilha 'tombados'
    try:
        tomb_sheet = consulta.worksheet("tombados")
    except:
        tomb_sheet = consulta.add_worksheet(title="tombados", rows="1000", cols="3")
        tomb_sheet.append_row(["cpf", "contrato", "timestamp"])
    tomb_sheet.append_row([cpf, contrato, timestamp])

    # Remove da planilha 'aguardando'
    try:
        aguard_sheet = consulta.worksheet("aguardando")
        all_values = aguard_sheet.get_all_values()
        header = all_values[0]
        data = all_values[1:]

        new_data = [row for row in data if not (row[0] == cpf and row[1] == contrato)]

        aguard_sheet.clear()
        aguard_sheet.append_row(header)
        for row in new_data:
            aguard_sheet.append_row(row)
    except Exception as e:
        st.warning(f"Erro ao remover de 'aguardando': {e}")

    st.cache_data.clear()
    st.session_state['aguardando_set'] = carregar_aguardando_google()  # Limpa cache das planilhas


def marcar_cpf_ativo(cpf):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet = client.open("consulta_ativa").sheet1 # Get the sheet reference again
    sheet.append_row([cpf, timestamp])
    st.cache_data.clear() # Invalidate cache for active CPFs

def marcar_aguardando(cpf, contrato):
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
    except:
        aguard_sheet = client.open("consulta_ativa").add_worksheet(title="aguardando", rows="1000", cols="3")
        aguard_sheet.append_row(["cpf", "contrato", "timestamp"])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    aguard_sheet.append_row([cpf, contrato, timestamp])
    st.cache_data.clear() # Invalidate cache for aguardando data

# Initialize session state variables
for key in ["autenticado", "arquivo_novo", "arquivo_tomb", "novo_df", "tomb_df", "ultimo_cpf_consultado"]:
    if key not in st.session_state:
        st.session_state[key] = None if key not in ["autenticado", "novo_df", "tomb_df"] else False if key == "autenticado" else pd.DataFrame()

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

if not st.session_state.autenticado:
    autenticar()
    st.stop()

@st.cache_data
def formatar_documentos(df_input, col, tamanho):
    df = df_input.copy() # Work on a copy to avoid SettingWithCopyWarning
    df[col] = df[col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(tamanho)
    return df

@st.cache_data
def load_and_process_data(novo_path, tomb_path):
    novo_df = pd.read_excel(novo_path)
    tomb_df = pd.read_excel(tomb_path)

    novo_df = formatar_documentos(novo_df, 'Número CPF/CNPJ', 11)
    tomb_df = formatar_documentos(tomb_df, 'CPF Tomador', 11)
    if 'Número Contrato' in tomb_df.columns:
        tomb_df['Número Contrato'] = tomb_df['Número Contrato'].astype(str)
    if 'Número Contrato Crédito' in novo_df.columns:
        novo_df['Número Contrato Crédito'] = novo_df['Número Contrato Crédito'].astype(str)

    return novo_df, tomb_df

def salvar_arquivos(upload_novo, upload_tomb):
    with open(NOVO_PATH, "wb") as f:
        f.write(upload_novo.read())
    with open(TOMB_PATH, "wb") as f:
        f.write(upload_tomb.read())
    # Invalidate caches that depend on these files
    st.cache_data.clear() # Clears all @st.cache_data caches
    # Re-load processed data into session state
    st.session_state.novo_df, st.session_state.tomb_df = load_and_process_data(NOVO_PATH, TOMB_PATH)


# --- Data Loading and Pre-processing (Centralized and Cached) ---
if not os.path.exists(NOVO_PATH) or not os.path.exists(TOMB_PATH):
    st.info("Faça o upload das bases para iniciar o sistema.")
    arquivo_novo = st.file_uploader("Base NovoEmprestimo.xlsx", type="xlsx", key="upload_novo")
    arquivo_tomb = st.file_uploader("Base Tombamento.xlsx", type="xlsx", key="upload_tomb")
    if arquivo_novo and arquivo_tomb:
        salvar_arquivos(arquivo_novo, arquivo_tomb)
        st.success("Bases carregadas com sucesso.")
        st.cache_data.clear()
st.rerun()
    else:
        st.stop()
else:
    # Load data once and store in session state
    if st.session_state.novo_df.empty or st.session_state.tomb_df.empty:
        st.session_state.novo_df, st.session_state.tomb_df = load_and_process_data(NOVO_PATH, TOMB_PATH)

# Retrieve data for calculations and display
df = st.session_state.novo_df
tomb = st.session_state.tomb_df
cpfs_ativos = carregar_cpfs_ativos()
tombados = carregar_tombados_google()
aguardando = carregar_aguardando_google()

# Filter initial DataFrame once for common conditions
@st.cache_data
def get_filtered_df(df_input):
    return df_input[
        (df_input['Submodalidade Bacen'] == 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.') &
        (df_input['Critério Débito'] == 'FOLHA DE PAGAMENTO') &
        (~df_input['Código Linha Crédito'].isin([140073, 138358, 141011, 101014, 137510]))
    ].copy()

filtered_common_df = get_filtered_df(df)

# --- Optimized Calculation of Counts for Menu Items ---
@st.cache_data
def calculate_counts(filtered_df, tomb_df, active_cpfs, tombados_set, aguardando_set):
    # Inconsistencies count
    merged_df = filtered_df.merge(
        tomb_df[['CPF Tomador', 'Número Contrato']],
        left_on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        right_on=['CPF Tomador', 'Número Contrato'],
        how='left',
        indicator=True
    )
    inconsistencias_df = merged_df[merged_df['_merge'] == 'left_only']
    num_inconsistencias = len(inconsistencias_df)

    # Registros Consulta Ativa count
    # Convert sets to DataFrames for merging if they are large, otherwise list comprehension is fine
    # For optimization, let's create a temporary DataFrame for faster lookup
    active_contracts_df = filtered_df[
        filtered_df['Número CPF/CNPJ'].isin(active_cpfs)
    ].copy()
    
    # Exclude already tombados or aguardando
    active_contracts_df['temp_key'] = active_contracts_df.apply(lambda r: (r['Número CPF/CNPJ'], r['Número Contrato Crédito']), axis=1)
    
    registros_consulta_ativa_df = active_contracts_df[
        ~active_contracts_df['temp_key'].isin(tombados_set) &
        ~active_contracts_df['temp_key'].isin(aguardando_set)
    ].drop(columns=['temp_key'])
    num_consulta_ativa = len(registros_consulta_ativa_df)


    # Aguardando Conclusão count (ajustado para excluir tombados)
aguardando_df = pd.DataFrame(list(aguardando_set), columns=['Número CPF/CNPJ', 'Número Contrato Crédito'])

# Exclui registros que já foram tombados
aguardando_df['temp_key'] = list(zip(aguardando_df['Número CPF/CNPJ'], aguardando_df['Número Contrato Crédito']))
aguardando_df = aguardando_df[~aguardando_df['temp_key'].isin(tombados_set)].drop(columns=['temp_key'])

merged_aguardando = aguardando_df.merge(
    df,
    on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
    how='inner'
)
    num_aguardando = len(merged_st.session_state.get('aguardando_set', aguardando))

    # Tombado count
    tombados_df_temp = pd.DataFrame(list(tombados_set), columns=['Número CPF/CNPJ', 'Número Contrato Crédito'])
    merged_tombados = tombados_df_temp.merge(
        df,
        on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        how='inner'
    )
    num_tombado = len(merged_tombados)

    return num_inconsistencias, num_consulta_ativa, num_aguardando, num_tombado, inconsistencias_df, registros_consulta_ativa_df, merged_aguardando, merged_tombados

num_inconsistencias, num_consulta_ativa, num_aguardando, num_tombado, inconsistencias_data, registros_consulta_ativa_data, aguardando_conclusao_data, tombado_data = \
    calculate_counts(filtered_common_df, tomb, cpfs_ativos, tombados, st.session_state.get('aguardando_set', aguardando))

st.sidebar.header("Menu")
menu_options = [
    "Consulta Individual",
    f"Registros Consulta Ativa ({num_consulta_ativa})",
    f"Aguardando Conclusão ({num_aguardando})",
    f"Tombado ({num_tombado})",
    "Resumo",
    f"Inconsistências ({num_inconsistencias})",
    "Atualizar Bases"
]
menu = st.sidebar.radio("Navegação", menu_options)

if menu == "Atualizar Bases":
    st.session_state.arquivo_novo = st.sidebar.file_uploader("Nova Base NovoEmprestimo.xlsx", type="xlsx")
    st.session_state.arquivo_tomb = st.sidebar.file_uploader("Nova Base Tombamento.xlsx", type="xlsx")
    if st.sidebar.button("Atualizar"):
        if st.session_state.arquivo_novo and st.session_state.arquivo_tomb:
            salvar_arquivos(st.session_state.arquivo_novo, st.session_state.arquivo_tomb)
            st.success("Bases atualizadas.")
            st.cache_data.clear()
st.rerun() # Rerun to update counts and dataframes
        else:
            st.warning("Envie os dois arquivos para atualizar.")
    st.stop()


if "Consulta Individual" in menu:
    st.title("🔍 Consulta de Empréstimos por CPF")
    cpf_input = st.text_input("Digite o CPF (apenas números):", key="cpf_consulta").strip()

    if "ultimo_cpf_consultado" not in st.session_state:
        st.session_state.ultimo_cpf_consultado = None

    if st.button("Consultar"):
        st.session_state.ultimo_cpf_consultado = cpf_input

    if st.session_state.ultimo_cpf_consultado:
        cpf_validado = st.session_state.ultimo_cpf_consultado

        if cpf_validado and len(cpf_validado) == 11 and cpf_validado.isdigit():
            # Use the already filtered common_df
            filtrado = filtered_common_df[filtered_common_df['Número CPF/CNPJ'] == cpf_validado].copy()

            if filtrado.empty:
                st.warning("Nenhum contrato encontrado com os filtros aplicados.")
            else:
                # Optimized merge for consignante info
                resultados_df = filtrado.merge(
                    tomb[['CPF Tomador', 'Número Contrato', 'CNPJ Empresa Consignante', 'Empresa Consignante']],
                    left_on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
                    right_on=['CPF Tomador', 'Número Contrato'],
                    how='left'
                )
                resultados_df['Consignante'] = resultados_df['CNPJ Empresa Consignante'].fillna("CONSULTE SISBR")
                resultados_df['Empresa Consignante'] = resultados_df['Empresa Consignante'].fillna("CONSULTE SISBR")

                display_cols = [
                    "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito",
                    "Quantidade Parcelas Abertas", "% Taxa Operação", "Código Linha Crédito",
                    "Nome Comercial", "Consignante", "Empresa Consignante"
                ]
                st.dataframe(resultados_df[display_cols])

                if cpf_validado in cpfs_ativos:
                    st.info("✅ CPF já marcado como Consulta Ativa.")
                else:
                    if st.button("Marcar como Consulta Ativa"):
                        marcar_cpf_ativo(cpf_validado)
                        st.success("✅ CPF marcado com sucesso.")
                        st.cache_data.clear()
st.rerun()
        else:
            st.warning("CPF inválido. Digite exatamente 11 números.")


if "Registros Consulta Ativa" in menu:
    st.title(f"📋 Registros de Consulta Ativa ({num_consulta_ativa})")

    if not registros_consulta_ativa_data.empty:
        st.dataframe(registros_consulta_ativa_data, use_container_width=True)

        # Get unique CPFs and Contracts from the already calculated and filtered dataframe
        unique_cpfs_ca = registros_consulta_ativa_data['Número CPF/CNPJ'].unique().tolist()
        cpf_escolhido = st.selectbox("CPF para marcar como Lançado Sisbr", unique_cpfs_ca, key="cpf_ca_key")

        contratos_filtrados = registros_consulta_ativa_data[
            registros_consulta_ativa_data['Número CPF/CNPJ'] == cpf_escolhido
        ]['Número Contrato Crédito'].astype(str).tolist()

        contrato_escolhido = st.selectbox("Contrato para marcar:", contratos_filtrados, key=f"contrato_ca_{cpf_escolhido}")

        if st.button("Marcar como Lançado Sisbr", key=f"btn_ca_{cpf_escolhido}_{contrato_escolhido}"):
            marcar_aguardando(cpf_escolhido, contrato_escolhido)
            st.success(f"Contrato {contrato_escolhido} do CPF {cpf_escolhido} foi movido para 'Aguardando Conclusão'.")
            st.cache_data.clear()
st.rerun()
    else:
        st.info("Nenhum registro disponível para Consulta Ativa.")


if menu == "Resumo":
    st.title("📊 Resumo Consolidado por Consignante (Base Completa)")

    if not filtered_common_df.empty:
        # Prepare data for merge and status flags
        temp_df = filtered_common_df[['Número CPF/CNPJ', 'Número Contrato Crédito']].copy()
        temp_df['Contrato_Tuple'] = list(zip(temp_df['Número CPF/CNPJ'], temp_df['Número Contrato Crédito']))

        # Add status columns directly
        temp_df['Consulta Ativa'] = temp_df['Número CPF/CNPJ'].isin(cpfs_ativos)
        temp_df['Tombado'] = temp_df['Contrato_Tuple'].isin(tombados)
        temp_df['Aguardando'] = temp_df['Contrato_Tuple'].isin(st.session_state.get('aguardando_set', aguardando))

        # Merge with tomb for consignante info
        df_registros = temp_df.merge(
            tomb[['CPF Tomador', 'Número Contrato', 'CNPJ Empresa Consignante', 'Empresa Consignante']],
            left_on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
            right_on=['CPF Tomador', 'Número Contrato'],
            how='left'
        )
        df_registros['CNPJ Empresa Consignante'] = df_registros['CNPJ Empresa Consignante'].fillna("CONSULTE SISBR")
        df_registros['Empresa Consignante'] = df_registros['Empresa Consignante'].fillna("CONSULTE SISBR")
        df_registros = df_registros.rename(columns={'Número CPF/CNPJ': 'CPF', 'Número Contrato Crédito': 'Contrato'})


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
                    df_registros[['CPF', 'Contrato', 'CNPJ Empresa Consignante', 'Empresa Consignante', 'Consulta Ativa', 'Tombado', 'Aguardando']].to_excel(writer, index=False, sheet_name="Relação Analítica")
                buffer.seek(0)
                st.download_button(
                    label="Exportar para Excel",
                    data=buffer,
                    file_name="resumo_analitico.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    else:
        st.info("Nenhum dado encontrado na base para resumo.")


if "Inconsistências" in menu:
    st.title(f"🚨 Contratos sem Correspondência no Tombamento ({num_inconsistencias})")

    if inconsistencias_data.empty:
        st.success("Nenhuma inconsistência encontrada.")
    else:
        st.warning(f"{len(inconsistencias_data)} contratos sem correspondência no tombamento encontrados.")
        # Only show relevant columns for inconsistencies
        st.dataframe(inconsistencias_data[
            ['Número CPF/CNPJ', 'Número Contrato Crédito', 'Código Linha Crédito', 'Nome Cliente']
        ])

        with st.expander("📥 Exportar inconsistências"):
            import io
            with io.BytesIO() as buffer:
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    inconsistencias_data[[
                        'Número CPF/CNPJ', 'Número Contrato Crédito', 'Código Linha Crédito', 'Nome Cliente'
                    ]].to_excel(writer, index=False, sheet_name="Inconsistencias")
                buffer.seek(0)
                st.download_button(
                    label="Exportar para Excel",
                    data=buffer,
                    file_name="inconsistencias_tombamento.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )


if "Aguardando Conclusão" in menu:
    st.title(f"⏳ Registros Aguardando Conclusão ({num_aguardando})")

    if not aguardando_conclusao_data.empty:
        st.dataframe(aguardando_conclusao_data, use_container_width=True)

        unique_cpfs_ag = aguardando_conclusao_data['Número CPF/CNPJ'].unique().tolist()
        cpf_escolhido = st.selectbox("CPF para tombar", unique_cpfs_ag, key="cpf_ag_key")

        contratos_filtrados = aguardando_conclusao_data[
            aguardando_conclusao_data['Número CPF/CNPJ'] == cpf_escolhido
        ]['Número Contrato Crédito'].astype(str).tolist()

        contrato_escolhido = st.selectbox("Contrato para tombar:", contratos_filtrados, key=f"contrato_ag_{cpf_escolhido}")

        if st.button("Marcar como Tombado", key=f"btn_ag_{cpf_escolhido}_{contrato_escolhido}"):
            marcar_tombado(cpf_escolhido, contrato_escolhido)
            st.success(f"Contrato {contrato_escolhido} do CPF {cpf_escolhido} foi tombado com sucesso.")
            st.cache_data.clear()
st.rerun()
    else:
        st.info("Nenhum registro encontrado.")


if "Tombado" in menu:
    st.title(f"📁 Registros Tombados ({num_tombado})")

    if not tombado_data.empty:
        # Merge with tomb for consignante info for display
        df_resultado = tombado_data.merge(
            tomb[['CPF Tomador', 'Número Contrato', 'CNPJ Empresa Consignante', 'Empresa Consignante']],
            left_on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
            right_on=['CPF Tomador', 'Número Contrato'],
            how='left'
        )
        df_resultado['Consignante'] = df_resultado['CNPJ Empresa Consignante'].fillna("CONSULTE SISBR")
        df_resultado['Empresa Consignante'] = df_resultado['Empresa Consignante'].fillna("CONSULTE SISBR")

        display_cols_tomb = [
            "Número CPF/CNPJ", "Nome Cliente", "Número Contrato Crédito",
            "Quantidade Parcelas Abertas", "% Taxa Operação", "Código Linha Crédito",
            "Nome Comercial", "Consignante", "Empresa Consignante"
        ]
        st.dataframe(df_resultado[display_cols_tomb], use_container_width=True)

        cpfs_disponiveis_tomb = df_resultado['Número CPF/CNPJ'].unique().tolist()
        if cpfs_disponiveis_tomb:
            cpf_escolhido_tomb = st.selectbox("Selecione o CPF para visualizar contratos tombados:", sorted(list(set(cpfs_disponiveis_tomb))), key="select_cpf_tombado_view")
            
            contratos_do_cpf_tomb = df_resultado[df_resultado['Número CPF/CNPJ'] == cpf_escolhido_tomb]['Número Contrato Crédito'].astype(str).tolist()
            if contratos_do_cpf_tomb:
                contrato_escolhido_tomb = st.selectbox("Selecione o Contrato tombado:", sorted(list(set(contratos_do_cpf_tomb))), key="select_contrato_tombado_view")
                st.info(f"Detalhes do contrato {contrato_escolhido_tomb} para o CPF {cpf_escolhido_tomb} podem ser visualizados na tabela acima.")
            else:
                st.info("Nenhum contrato tombado para o CPF selecionado.")
        else:
            st.info("Nenhum CPF disponível para seleção.")
    else:
        st.info("Nenhum contrato marcado como tombado encontrado.")

