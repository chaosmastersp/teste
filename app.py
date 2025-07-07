import streamlit as st
import pandas as pd
import os
import json
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import easyocr
from PIL import Image
import re
import io
import numpy as np

st.set_page_config(page_title="Consulta de Empréstimos", layout="wide")

# --- Configuração do Google Sheets ---
@st.cache_resource
def get_gspread_client():
    """
    Retorna o cliente gspread autorizado.
    Armazenado em cache para evitar autenticação repetida.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_dict = json.loads(st.secrets["gspread"]["json"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Erro ao autenticar com Google Sheets. Verifique suas credenciais: {e}")
        st.stop() # Impede a execução do restante do script se a autenticação falhar

client = get_gspread_client()

# --- Funções de Carregamento de Dados do Google Sheets (com cache) ---
@st.cache_data(ttl=300) # Cache por 5 minutos
def carregar_cpfs_ativos():
    """Carrega CPFs ativos da planilha 'consulta_ativa'."""
    try:
        sheet = client.open("consulta_ativa").sheet1
        values = sheet.get_all_values()
        if not values or len(values) < 2:
            return []
        return [row[0] for row in values[1:]]
    except Exception as e:
        st.error(f"Erro ao carregar CPFs ativos do Google Sheets: {e}")
        return []

@st.cache_data(ttl=300)
def carregar_tombados_google():
    """Carrega registros tombados da planilha 'tombados'."""
    try:
        tomb_sheet = client.open("consulta_ativa").worksheet("tombados")
        values = tomb_sheet.get_all_values()
        if not values or len(values) < 2:
            return set()
        return set((row[0], row[1]) for row in values[1:])
    except Exception as e:
        st.error(f"Erro ao carregar registros tombados do Google Sheets: {e}")
        return set()

@st.cache_data(ttl=300)
def carregar_aguardando_google():
    """Carrega registros aguardando conclusão da planilha 'aguardando'."""
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
        values = aguard_sheet.get_all_values()
        if not values or len(values) < 2:
            return set()
        return set((row[0], row[1]) for row in values[1:])
    except Exception as e:
        st.error(f"Erro ao carregar registros aguardando do Google Sheets: {e}")
        return set()

# --- Funções de Modificação do Google Sheets (invalidam cache) ---
def marcar_tombado(cpf, contrato):
    """Marca um contrato como tombado e o remove da lista de aguardando."""
    try:
        tomb_sheet = client.open("consulta_ativa").worksheet("tombados")
    except gspread.exceptions.WorksheetNotFound:
        tomb_sheet = client.open("consulta_ativa").add_worksheet(title="tombados", rows="1000", cols="3")
        tomb_sheet.append_row(["cpf", "contrato", "timestamp"])

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tomb_sheet.append_row([cpf, contrato, timestamp])

    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
        data = aguard_sheet.get_all_values()
        if data: # Garante que há dados para processar
            header = data[0]
            rows = data[1:]
            nova_lista = [row for row in rows if not (row[0] == cpf and row[1] == contrato)]

            aguard_sheet.clear()
            aguard_sheet.append_row(header)
            if nova_lista: # Adiciona as linhas restantes apenas se houver
                aguard_sheet.append_rows(nova_lista)
    except gspread.exceptions.WorksheetNotFound:
        st.warning("A aba 'aguardando' não existe, não foi possível remover o registro.")
    except Exception as e:
        st.warning(f"Erro ao remover da aba aguardando: {e}")

    st.cache_data.clear() # Invalida todos os caches de dados para recarregar informações atualizadas

def marcar_cpf_ativo(cpf):
    """Marca um CPF como ativo na planilha principal."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet = client.open("consulta_ativa").sheet1
    sheet.append_row([cpf, timestamp])
    st.cache_data.clear()

def marcar_aguardando(cpf, contrato):
    """Marca um contrato como aguardando conclusão."""
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
    except gspread.exceptions.WorksheetNotFound:
        aguard_sheet = client.open("consulta_ativa").add_worksheet(title="aguardando", rows="1000", cols="3")
        aguard_sheet.append_row(["cpf", "contrato", "timestamp"])

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    aguard_sheet.append_row([cpf, contrato, timestamp])
    st.cache_data.clear()

# --- Inicialização do Session State ---
# Garante que as variáveis de estado existam
for key in ["autenticado", "novo_df", "tomb_df", "ultimo_cpf_consultado"]:
    if key not in st.session_state:
        if key == "autenticado":
            st.session_state[key] = False
        elif key in ["novo_df", "tomb_df"]:
            st.session_state[key] = pd.DataFrame()
        else:
            st.session_state[key] = None

DATA_DIR = "data"
NOVO_PATH = os.path.join(DATA_DIR, "novoemprestimo.xlsx")
TOMB_PATH = os.path.join(DATA_DIR, "tombamento.xlsx")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- Autenticação ---
def autenticar():
    """Função para autenticar o usuário com uma senha."""
    senha = st.text_input("Digite a senha para acessar o sistema:", type="password", key="senha_app")
    if senha == "tombamento":
        st.session_state.autenticado = True
        st.success("Acesso autorizado.")
        st.rerun() # Recarrega a página para remover o campo de senha
    elif senha:
        st.error("Senha incorreta.")

if not st.session_state.autenticado:
    autenticar()
    st.stop() # Impede a execução do restante do script se não autenticado

# --- Funções de Processamento de Dados Locais ---
@st.cache_data
def formatar_documentos(df_input, col, tamanho):
    """Formata colunas de documentos (CPF/CNPJ) para o tamanho especificado."""
    df = df_input.copy()
    df[col] = df[col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(tamanho)
    return df

@st.cache_data
def load_and_process_data(novo_path, tomb_path):
    """
    Carrega e processa os DataFrames de empréstimos e tombamento.
    Armazenado em cache para evitar recarregamento se os arquivos não mudarem.
    """
    try:
        novo_df = pd.read_excel(novo_path)
        tomb_df = pd.read_excel(tomb_path)

        novo_df = formatar_documentos(novo_df, 'Número CPF/CNPJ', 11)
        tomb_df = formatar_documentos(tomb_df, 'CPF Tomador', 11)

        if 'Número Contrato' in tomb_df.columns:
            tomb_df['Número Contrato'] = tomb_df['Número Contrato'].astype(str)
        if 'Número Contrato Crédito' in novo_df.columns:
            novo_df['Número Contrato Crédito'] = novo_df['Número Contrato Crédito'].astype(str)

        return novo_df, tomb_df
    except FileNotFoundError:
        st.error("Um ou ambos os arquivos de base não foram encontrados. Por favor, faça o upload.")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar ou processar os arquivos Excel: {e}")
        return pd.DataFrame(), pd.DataFrame()

def salvar_arquivos(upload_novo, upload_tomb):
    """Salva os arquivos carregados localmente e atualiza o session state."""
    try:
        with open(NOVO_PATH, "wb") as f:
            f.write(upload_novo.read())
        with open(TOMB_PATH, "wb") as f:
            f.write(upload_tomb.read())
        st.cache_data.clear() # Limpa o cache para forçar o recarregamento dos novos dados
        st.session_state.novo_df, st.session_state.tomb_df = load_and_process_data(NOVO_PATH, TOMB_PATH)
        st.success("Bases carregadas/atualizadas com sucesso.")
        st.rerun() # Recarrega a página para refletir os novos dados
    except Exception as e:
        st.error(f"Erro ao salvar os arquivos: {e}")

# --- Lógica de Carregamento Inicial dos Arquivos ---
# Verifica se os DataFrames já estão no session_state e não estão vazios
if st.session_state.novo_df.empty or st.session_state.tomb_df.empty:
    # Se não estiverem, tenta carregar dos arquivos locais
    if os.path.exists(NOVO_PATH) and os.path.exists(TOMB_PATH):
        st.session_state.novo_df, st.session_state.tomb_df = load_and_process_data(NOVO_PATH, TOMB_PATH)
        if st.session_state.novo_df.empty or st.session_state.tomb_df.empty:
            st.warning("Os arquivos locais foram encontrados, mas não puderam ser processados. Por favor, faça o upload novamente.")
            # Força o upload se o processamento falhou
            arquivo_novo = st.file_uploader("Base NovoEmprestimo.xlsx", type="xlsx", key="upload_novo_initial")
            arquivo_tomb = st.file_uploader("Base Tombamento.xlsx", type="xlsx", key="upload_tomb_initial")
            if arquivo_novo and arquivo_tomb:
                salvar_arquivos(arquivo_novo, arquivo_tomb)
            st.stop() # Para a execução até que os arquivos sejam carregados
    else:
        st.info("Faça o upload das bases para iniciar o sistema.")
        arquivo_novo = st.file_uploader("Base NovoEmprestimo.xlsx", type="xlsx", key="upload_novo_initial")
        arquivo_tomb = st.file_uploader("Base Tombamento.xlsx", type="xlsx", key="upload_tomb_initial")
        if arquivo_novo and arquivo_tomb:
            salvar_arquivos(arquivo_novo, arquivo_tomb)
        st.stop() # Para a execução até que os arquivos sejam carregados

# Se chegamos aqui, os DataFrames estão carregados no session_state
df = st.session_state.novo_df
tomb = st.session_state.tomb_df

# Verifica se os DataFrames estão realmente carregados antes de prosseguir
if df.empty or tomb.empty:
    st.error("Não foi possível carregar os dados. Verifique os arquivos e tente novamente.")
    st.stop()

# --- Carregamento de Dados do Google Sheets (sempre atualizados) ---
cpfs_ativos = carregar_cpfs_ativos()
tombados = carregar_tombados_google()
aguardando = carregar_aguardando_google()

# --- Filtragem Inicial do DataFrame (com cache) ---
@st.cache_data
def get_filtered_df(df_input):
    """
    Aplica os filtros comuns ao DataFrame principal.
    Armazenado em cache para evitar reprocessamento.
    """
    return df_input[
        (df_input['Submodalidade Bacen'] == 'CRÉDITO PESSOAL - COM CONSIGNAÇÃO EM FOLHA DE PAGAM.') &
        (df_input['Critério Débito'] == 'FOLHA DE PAGAMENTO') &
        (~df_input['Código Linha Crédito'].isin([140073, 138358, 141011, 101014, 137510]))
    ].copy()

filtered_common_df = get_filtered_df(df)

# --- Cálculo Otimizado de Contagens para o Menu (com cache) ---
@st.cache_data
def calculate_counts(filtered_df, tomb_df, active_cpfs, tombados_set, aguardando_set):
    """
    Calcula as contagens para os itens do menu.
    Armazenado em cache para otimização.
    """
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
    active_contracts_df = filtered_df[
        filtered_df['Número CPF/CNPJ'].isin(active_cpfs)
    ].copy()

    active_contracts_df['temp_key'] = active_contracts_df.apply(
        lambda r: (r['Número CPF/CNPJ'], r['Número Contrato Crédito']), axis=1
    )

    registros_consulta_ativa_df = active_contracts_df[
        ~active_contracts_df['temp_key'].isin(tombados_set) &
        ~active_contracts_df['temp_key'].isin(aguardando_set)
    ].drop(columns=['temp_key'])
    num_consulta_ativa = len(registros_consulta_ativa_df)

    # Aguardando Conclusão count
    # Cria um DataFrame temporário a partir do set para merge
    aguardando_df_temp = pd.DataFrame(list(aguardando_set), columns=['Número CPF/CNPJ', 'Número Contrato Crédito'])
    merged_aguardando = aguardando_df_temp.merge(
        df, # Usa o df completo para obter todas as colunas
        on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        how='inner'
    )
    num_aguardando = len(merged_aguardando)

    # Tombado count
    # Cria um DataFrame temporário a partir do set para merge
    tombados_df_temp = pd.DataFrame(list(tombados_set), columns=['Número CPF/CNPJ', 'Número Contrato Crédito'])
    merged_tombados = tombados_df_temp.merge(
        df, # Usa o df completo para obter todas as colunas
        on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        how='inner'
    )
    num_tombado = len(merged_tombados)

    return num_inconsistencias, num_consulta_ativa, num_aguardando, num_tombado, \
           inconsistencias_df, registros_consulta_ativa_df, merged_aguardando, merged_tombados

num_inconsistencias, num_consulta_ativa, num_aguardando, num_tombado, \
inconsistencias_data, registros_consulta_ativa_data, aguardando_conclusao_data, tombado_data = \
    calculate_counts(filtered_common_df, tomb, cpfs_ativos, tombados, aguardando)

# --- Menu Lateral ---
st.sidebar.header("Menu")
menu_options = [
    "Consulta Individual",
    f"Registros Consulta Ativa ({num_consulta_ativa})",
    f"Aguardando Conclusão ({num_aguardando})",
    f"Tombado ({num_tombado})",
    "Resumo",
    f"Inconsistências ({num_inconsistencias})",
    "Imagens",
    "Atualizar Bases"
]
menu = st.sidebar.radio("Navegação", menu_options)

# --- Seções do Aplicativo ---
# A ordem dos 'if/elif' importa para a lógica de 'st.stop()'
if menu == "Atualizar Bases":
    st.title("🔄 Atualizar Bases de Dados")
    st.info("Faça o upload de novas versões dos arquivos para atualizar o sistema.")
    arquivo_novo_update = st.file_uploader("Nova Base NovoEmprestimo.xlsx", type="xlsx", key="upload_novo_update")
    arquivo_tomb_update = st.file_uploader("Nova Base Tombamento.xlsx", type="xlsx", key="upload_tomb_update")
    if st.button("Atualizar Bases"):
        if arquivo_novo_update and arquivo_tomb_update:
            salvar_arquivos(arquivo_novo_update, arquivo_tomb_update)
            st.success("Bases atualizadas com sucesso!")
            st.rerun()
        else:
            st.warning("Por favor, envie ambos os arquivos para atualizar.")
    st.stop() # Impede a execução de outras seções após a atualização

elif menu == "Consulta Individual":
    st.title("🔍 Consulta de Empréstimos por CPF")
    cpf_input = st.text_input("Digite o CPF (apenas números):", key="cpf_consulta").strip()

    # Botão de consulta para acionar a lógica
    if st.button("Consultar CPF"):
        st.session_state.ultimo_cpf_consultado = cpf_input

    if st.session_state.ultimo_cpf_consultado:
        cpf_validado = st.session_state.ultimo_cpf_consultado

        if cpf_validado and len(cpf_validado) == 11 and cpf_validado.isdigit():
            filtrado = filtered_common_df[filtered_common_df['Número CPF/CNPJ'] == cpf_validado].copy()

            if filtrado.empty:
                st.warning("Nenhum contrato encontrado com os filtros aplicados para este CPF.")
            else:
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
                st.dataframe(resultados_df[display_cols], use_container_width=True)

                # Botão "Marcar como Consulta Ativa" condicional
                if cpf_validado in cpfs_ativos:
                    st.info("✅ CPF já marcado como Consulta Ativa.")
                else:
                    if st.button("Marcar como Consulta Ativa", key="btn_marcar_ca"):
                        marcar_cpf_ativo(cpf_validado)
                        st.success("✅ CPF marcado com sucesso.")
                        st.rerun()
        else:
            st.warning("CPF inválido. Digite exatamente 11 números.")

elif menu == "Registros Consulta Ativa":
    st.title(f"📋 Registros de Consulta Ativa ({num_consulta_ativa})")

    if not registros_consulta_ativa_data.empty:
        st.dataframe(registros_consulta_ativa_data, use_container_width=True)

        st.subheader("Marcar Contratos como 'Aguardando Conclusão'")
        cpf_input_ca = st.text_input("Digite o CPF para filtrar contratos:", key="cpf_ca_input").strip()

        contratos_filtrados_ca = []
        if cpf_input_ca and len(cpf_input_ca) == 11 and cpf_input_ca.isdigit():
            contratos_filtrados_ca = registros_consulta_ativa_data[
                registros_consulta_ativa_data['Número CPF/CNPJ'] == cpf_input_ca
            ]['Número Contrato Crédito'].astype(str).tolist()
            if not contratos_filtrados_ca:
                st.info(f"Nenhum contrato ativo encontrado para o CPF {cpf_input_ca}.")
        elif cpf_input_ca:
            st.warning("CPF inválido. Digite exatamente 11 números.")

        if contratos_filtrados_ca:
            contratos_escolhidos_ca = st.multiselect(
                "Selecione os contratos para marcar como 'Aguardando Conclusão':",
                contratos_filtrados_ca,
                key="multiselect_ca"
            )

            # Botão "Marcar como Lançado Sisbr" condicional
            if contratos_escolhidos_ca:
                if st.button("Marcar como Lançado Sisbr", key="btn_marcar_sisbr"):
                    for contrato in contratos_escolhidos_ca:
                        marcar_aguardando(cpf_input_ca, contrato)
                    st.success(f"{len(contratos_escolhidos_ca)} contrato(s) marcado(s) como 'Aguardando Conclusão'.")
                    st.rerun()
            else:
                st.info("Selecione um ou mais contratos para habilitar o botão de marcação.")
    else:
        st.info("Nenhum registro disponível para Consulta Ativa.")

elif menu == "Resumo":
    st.title("📊 Resumo Consolidado por Consignante (Base Completa)")

    if not filtered_common_df.empty:
        temp_df = filtered_common_df[['Número CPF/CNPJ', 'Número Contrato Crédito']].copy()
        temp_df['Contrato_Tuple'] = list(zip(temp_df['Número CPF/CNPJ'], temp_df['Número Contrato Crédito']))

        temp_df['Consulta Ativa'] = temp_df['Número CPF/CNPJ'].isin(cpfs_ativos)
        temp_df['Tombado'] = temp_df['Contrato_Tuple'].isin(tombados)
        temp_df['Aguardando'] = temp_df['Contrato_Tuple'].isin(aguardando)

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

        st.dataframe(resumo, use_container_width=True)

        with st.expander("📥 Exportar relação analítica"):
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

elif menu == "Inconsistências":
    st.title(f"🚨 Contratos sem Correspondência no Tombamento ({num_inconsistencias})")

    if inconsistencias_data.empty:
        st.success("Nenhuma inconsistência encontrada.")
    else:
        st.warning(f"{len(inconsistencias_data)} contratos sem correspondência no tombamento encontrados.")
        st.dataframe(inconsistencias_data[
            ['Número CPF/CNPJ', 'Número Contrato Crédito', 'Código Linha Crédito', 'Nome Cliente']
        ], use_container_width=True)

        with st.expander("📥 Exportar inconsistências"):
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

elif menu == "Aguardando Conclusão":
    st.title(f"⏳ Registros Aguardando Conclusão ({num_aguardando})")

    if not aguardando_conclusao_data.empty:
        st.dataframe(aguardando_conclusao_data, use_container_width=True)

        st.subheader("Marcar Contratos como 'Tombado'")
        cpf_input_ag = st.text_input("Digite o CPF para filtrar contratos:", key="cpf_ag_input").strip()

        contratos_filtrados_ag = []
        if cpf_input_ag and len(cpf_input_ag) == 11 and cpf_input_ag.isdigit():
            contratos_filtrados_ag = aguardando_conclusao_data[
                aguardando_conclusao_data['Número CPF/CNPJ'] == cpf_input_ag
            ]['Número Contrato Crédito'].astype(str).tolist()
            if not contratos_filtrados_ag:
                st.info(f"Nenhum contrato aguardando conclusão encontrado para o CPF {cpf_input_ag}.")
        elif cpf_input_ag:
            st.warning("CPF inválido. Digite exatamente 11 números.")

        if contratos_filtrados_ag:
            contratos_escolhidos_ag = st.multiselect(
                "Selecione os contratos para marcar como 'Tombado':",
                contratos_filtrados_ag,
                key="multiselect_ag"
            )

            # Botão "Marcar como Tombado" condicional
            if contratos_escolhidos_ag:
                if st.button("Marcar como Tombado", key="btn_marcar_tombado"):
                    for contrato in contratos_escolhidos_ag:
                        marcar_tombado(cpf_input_ag, contrato)
                    st.success(f"{len(contratos_escolhidos_ag)} contrato(s) tombado(s) com sucesso.")
                    st.rerun()
            else:
                st.info("Selecione um ou mais contratos para habilitar o botão de marcação.")
    else:
        st.info("Nenhum registro encontrado na lista de 'Aguardando Conclusão'.")

elif menu == "Tombado":
    st.title(f"📁 Registros Tombados ({num_tombado})")

    if not tombado_data.empty:
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
    else:
        st.info("Nenhum contrato marcado como tombado encontrado.")

# --- Funções de Validação e Correção de CPF ---
def validar_cpf(cpf):
    """Valida um CPF."""
    cpf = ''.join(filter(str.isdigit, cpf))
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    for i in range(9, 11):
        soma = sum(int(cpf[j]) * ((i+1) - j) for j in range(i))
        digito = ((soma * 10) % 11) % 10
        if digito != int(cpf[i]):
            return False
    return True

def tentar_corrigir_cpf(cpf_raw):
    """Tenta corrigir um CPF com erros comuns de digitação."""
    substituicoes = {'1': '4', '4': '1', '0': '8', '8': '0', '5': '6', '6': '5'} # Adicione mais se necessário
    for i, c in enumerate(cpf_raw):
        if c in substituicoes:
            corrigido = list(cpf_raw)
            corrigido[i] = substituicoes[c]
            corrigido_str = "".join(corrigido)
            if validar_cpf(corrigido_str):
                return corrigido_str
    return None

# --- Configuração do EasyOCR ---
os.environ["EASYOCR_MODEL_STORAGE_DIR"] = "./.easyocr"
try:
    reader = easyocr.Reader(['pt'], gpu=False)
except Exception as e:
    st.error(f"Erro ao inicializar EasyOCR. Verifique a instalação e dependências: {e}")
    reader = None # Define reader como None para evitar erros posteriores

def extrair_cpfs_de_imagem(imagem):
    """Extrai CPFs de uma imagem usando EasyOCR."""
    if reader is None:
        st.error("EasyOCR não foi inicializado. Não é possível extrair CPFs de imagens.")
        return []
    try:
        imagem_np = np.array(imagem)
        result = reader.readtext(imagem_np)
        texto = " ".join([res[1] for res in result])
        # Regex para CPFs no formato XXX.XXX.XXX-XX ou apenas 11 dígitos
        cpfs_encontrados = re.findall(r'\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11}', texto)
        return cpfs_encontrados
    except Exception as e:
        st.error(f"Erro ao processar imagem com EasyOCR: {e}")
        return []

elif menu == "Imagens":
    st.title("📷 Extração de CPFs via Imagem")
    st.info("Envie imagens contendo CPFs para que o sistema tente extraí-los e marcá-los como 'Consulta Ativa'.")
    imagens = st.file_uploader("Envie uma ou mais imagens (PNG, JPG, JPEG)", type=["png", "jpg", "jpeg"], accept_multiple_files=True)

    if imagens:
        resultados = []
        for img_file in imagens:
            try:
                imagem = Image.open(img_file)
                cpfs_extraidos = extrair_cpfs_de_imagem(imagem)

                if not cpfs_extraidos:
                    resultados.append((img_file.name, "Nenhum CPF detectado na imagem."))
                    continue

                for cpf_raw in cpfs_extraidos:
                    cpf_limpo = re.sub(r'\D', '', cpf_raw) # Remove pontos e traços

                    status_msg = ""
                    if len(cpf_limpo) == 11 and validar_cpf(cpf_limpo):
                        if cpf_limpo in df['Número CPF/CNPJ'].values:
                            if cpf_limpo not in cpfs_ativos:
                                marcar_cpf_ativo(cpf_limpo)
                                status_msg = "✅ Marcado com sucesso"
                            else:
                                status_msg = "ℹ️ Já estava marcado"
                        else:
                            status_msg = "❌ CPF não encontrado na base de empréstimos"
                    else:
                        # Tenta corrigir se o CPF não é válido ou tem tamanho errado
                        cpf_corrigido = tentar_corrigir_cpf(cpf_limpo)
                        if cpf_corrigido and cpf_corrigido in df['Número CPF/CNPJ'].values:
                            if cpf_corrigido not in cpfs_ativos:
                                marcar_cpf_ativo(cpf_corrigido)
                                status_msg = f"✅ Corrigido ({cpf_limpo} ➜ {cpf_corrigido}) e marcado"
                            else:
                                status_msg = f"ℹ️ Corrigido ({cpf_limpo} ➜ {cpf_corrigido}), já estava marcado"
                        else:
                            status_msg = f"❌ CPF inválido ou não encontrado ({cpf_limpo})"

                    resultados.append((cpf_raw, status_msg))

            except Exception as e:
                resultados.append((img_file.name, f"Erro ao processar imagem: {e}"))

        if resultados:
            st.subheader("📄 Log de Processamento")
            df_resultados = pd.DataFrame(resultados, columns=["CPF Detectado", "Status"])
            st.dataframe(df_resultados, use_container_width=True)

            with io.BytesIO() as buffer:
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_resultados.to_excel(writer, index=False, sheet_name="Log CPFs Imagem")
                buffer.seek(0)
                st.download_button(
                    label="📥 Baixar log em Excel",
                    data=buffer,
                    file_name="log_cpfs_imagem.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
