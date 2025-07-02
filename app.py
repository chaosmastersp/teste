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
        # Normaliza os valores ao carregar para garantir consistência
        return set((str(row[0]).strip(), str(row[1]).strip()) for row in values[1:])  # (cpf, contrato)
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
        # Normaliza os valores ao carregar para garantir consistência
        return set((str(row[0]).strip(), str(row[1]).strip()) for row in values[1:])
    except Exception as e:
        st.error(f"Erro ao carregar registros aguardando: {e}")
        return set()

# Functions that modify Google Sheets should not be cached, but their calls should invalidate relevant caches
def marcar_tombado(cpf, contrato):
    consulta = client.open("consulta_ativa")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    st.info(f"DEBUG: Tentando tombar CPF: '{cpf}', Contrato: '{contrato}'") # Mensagem de depuração

    # Adiciona à planilha 'tombados'
    try:
        tomb_sheet = consulta.worksheet("tombados")
    except gspread.exceptions.WorksheetNotFound: # Use specific exception for clarity
        st.warning("DEBUG: Planilha 'tombados' não encontrada. Criando nova planilha.")
        tomb_sheet = consulta.add_worksheet(title="tombados", rows="1000", cols="3")
        tomb_sheet.append_row(["cpf", "contrato", "timestamp"])
    except Exception as e:
        st.error(f"ERRO CRÍTICO: Não foi possível acessar/criar planilha 'tombados': {e}")
        st.rerun() # Stop execution if essential sheet cannot be accessed
        return # Exit function on critical error
        
    tomb_sheet.append_row([cpf, contrato, timestamp])
    st.success(f"DEBUG: Adicionado '{contrato}' do CPF '{cpf}' à planilha 'tombados'.") # Mensagem de depuração

    # Remove da planilha 'aguardando'
    try:
        aguard_sheet = consulta.worksheet("aguardando")
        all_values = aguard_sheet.get_all_values()
        
        if not all_values: # This handles an empty sheet completely
            st.warning("DEBUG: Planilha 'aguardando' está vazia. Nenhuma linha para remover.")
            # Ensure header is present if sheet was completely empty
            try:
                aguard_sheet.clear()
                aguard_sheet.append_row(["cpf", "contrato", "timestamp"])
            except Exception as e_clear:
                st.error(f"ERRO: Não foi possível limpar ou adicionar cabeçalho em 'aguardando': {e_clear}")
            
            # Re-run after potential clear/header add even if no specific row was removed
            st.cache_data.clear()
            st.session_state['aguardando_set'] = carregar_aguardando_google()
            st.session_state['tombados_set'] = carregar_tombados_google()
            st.rerun()
            return # Exit function as nothing specific needs removal

        header = all_values[0]
        data = all_values[1:] # Actual data rows
        
        st.info(f"DEBUG: Dados lidos de 'aguardando' (exceto cabeçalho): {data}")
        
        found_and_removed = False # Flag to check if item was found and effectively "removed" from data
        new_data = []
        # Normaliza o CPF e Contrato de entrada para comparação
        target_cpf = str(cpf).strip()
        target_contrato = str(contrato).strip()

        st.info(f"DEBUG: Procurando por (CPF: '{target_cpf}', Contrato: '{target_contrato}') para remover.") # Mensagem de depuração
        
        for row in data:
            # Normaliza os valores da linha do sheet para comparação
            current_cpf_in_sheet = str(row[0]).strip()
            current_contrato_in_sheet = str(row[1]).strip()
            
            st.info(f"DEBUG: Comparando sheet row (CPF: '{current_cpf_in_sheet}', Contrato: '{current_contrato_in_sheet}') com target (CPF: '{target_cpf}', Contrato: '{target_contrato}')")

            if current_cpf_in_sheet == target_cpf and current_contrato_in_sheet == target_contrato:
                st.info(f"DEBUG: Correspondência encontrada para remover: CPF '{current_cpf_in_sheet}', Contrato '{current_contrato_in_sheet}'")
                found_and_removed = True
            else:
                new_data.append(row)
        
        if not found_and_removed:
            st.warning(f"DEBUG: Contrato (CPF: '{target_cpf}', Contrato: '{target_contrato}') NÃO encontrado na planilha 'aguardando' para remoção. Verifique os valores na planilha e no input.")

        st.info(f"DEBUG: Dados restantes em 'aguardando' após a tentativa de remoção: {new_data}")

        # Recria a planilha com cabeçalho + dados válidos
        values_to_update = [header] + new_data
        
        st.info("DEBUG: Tentando aguard_sheet.clear()")
        aguard_sheet.clear() # Clear the entire sheet
        st.info("DEBUG: aguard_sheet.clear() concluído.")
        
        if values_to_update and len(values_to_update) > 0: # Ensure header is always written, and data if present
            st.info(f"DEBUG: Tentando aguard_sheet.update('A1', {len(values_to_update)} linhas)")
            aguard_sheet.update("A1", values_to_update)
            st.info("DEBUG: aguard_sheet.update() concluído.")
            st.success(f"DEBUG: Contrato '{contrato}' do CPF '{cpf}' removido da planilha 'aguardando' com sucesso.")
        else: # This case should ideally not be hit if header is always included, but as a safeguard
            st.warning("DEBUG: `values_to_update` estava vazio ou apenas com cabeçalho. Planilha 'aguardando' pode estar vazia.")
            aguard_sheet.append_row(header) # Ensure header is present if nothing else is
            st.success(f"DEBUG: Contrato '{contrato}' do CPF '{cpf}' removido. Planilha 'aguardando' agora contém apenas o cabeçalho.")
            
        # --- Verificação Pós-Remoção ---
        st.info("DEBUG: Verificando se o registro foi realmente removido do Google Sheet...")
        aguard_sheet_reloaded = client.open("consulta_ativa").worksheet("aguardando")
        reloaded_values = aguard_sheet_reloaded.get_all_values()
        
        # Normaliza os valores recarregados para verificação
        reloaded_set = set((str(row[0]).strip(), str(row[1]).strip()) for row in reloaded_values[1:]) if len(reloaded_values) > 1 else set()

        if (target_cpf, target_contrato) not in reloaded_set:
            st.success(f"DEBUG: Confirmação: Registro (CPF: '{target_cpf}', Contrato: '{target_contrato}') NÃO encontrado na planilha 'aguardando' após a remoção.")
        else:
            st.error(f"DEBUG: ALERTA: Registro (CPF: '{target_cpf}', Contrato: '{target_contrato}') AINDA ENCONTRADO na planilha 'aguardando' após a remoção. Pode haver um problema de sincronização ou permissão.")


    except Exception as e:
        st.error(f"ERRO CRÍTICO ao remover de 'aguardando': {e}")
        st.exception(e) # Show full traceback for better debugging

    st.cache_data.clear()
    st.session_state['aguardando_set'] = carregar_aguardando_google()
    st.session_state['tombados_set'] = carregar_tombados_google()
    st.rerun()


def marcar_todos_contratos_tombados(cpf):
    try:
        consulta = client.open("consulta_ativa")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        target_cpf = str(cpf).strip().zfill(11)

        # Acessa planilha aguardando
        aguard_sheet = consulta.worksheet("aguardando")
        all_values = aguard_sheet.get_all_values()
        if not all_values or len(all_values) < 2:
            st.warning("Planilha 'aguardando' está vazia ou sem dados.")
            return

        header = all_values[0]
        data = all_values[1:]

        # Força normalização dos dados lidos
        novos_dados = []
        contratos_do_cpf = []

        for row in data:
            row_cpf = str(row[0]).strip().zfill(11)
            row_contrato = str(row[1]).strip()
            if row_cpf == target_cpf:
                contratos_do_cpf.append((row_cpf, row_contrato))
            else:
                novos_dados.append([row_cpf, row_contrato, row[2] if len(row) > 2 else ""])

        # Atualiza planilha aguardando com os dados restantes
        aguard_sheet.update("A1", [header] + novos_dados)

        # Acessa (ou cria) planilha 'tombados'
        try:
            tomb_sheet = consulta.worksheet("tombados")
        except gspread.exceptions.WorksheetNotFound:
            tomb_sheet = consulta.add_worksheet(title="tombados", rows="1000", cols="3")
            tomb_sheet.append_row(["cpf", "contrato", "timestamp"])

        # Adiciona os contratos tombados
        for cpf_row, contrato_row in contratos_do_cpf:
            tomb_sheet.append_row([cpf_row, contrato_row, timestamp])

        st.success(f"{len(contratos_do_cpf)} contrato(s) do CPF {target_cpf} foram tombados.")
        st.cache_data.clear()
        st.rerun()

    except Exception as e:
        st.error(f"Erro ao tombar contratos do CPF {cpf}: {e}")
        st.exception(e)

    except Exception as e:
        st.error(f"Erro ao tombar contratos do CPF {cpf}: {e}")
        st.exception(e)

    except Exception as e:
        st.error(f"Erro ao tombar contratos do CPF {cpf}: {e}")
        st.exception(e)


def marcar_cpf_ativo(cpf):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet = client.open("consulta_ativa").sheet1 # Get the sheet reference again
    sheet.append_row([cpf, timestamp])
    st.cache_data.clear() # Invalidate cache for active CPFs
    st.rerun() # Ensure UI updates

def marcar_aguardando(cpf, contrato):
    try:
        aguard_sheet = client.open("consulta_ativa").worksheet("aguardando")
    except:
        aguard_sheet = client.open("consulta_ativa").add_worksheet(title="aguardando", rows="1000", cols="3")
        aguard_sheet.append_row(["cpf", "contrato", "timestamp"])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    aguard_sheet.append_row([cpf, contrato, timestamp])
    st.cache_data.clear() # Invalidate cache for aguardando data
    st.rerun() # Ensure UI updates

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
        st.rerun() # Rerun to update counts and dataframes
    else:
        st.stop()
else:
    # Load data once and store in session state
    if st.session_state.novo_df.empty or st.session_state.tomb_df.empty:
        st.session_state.novo_df, st.session_state.tomb_df = load_and_process_data(NOVO_PATH, TOMB_PATH)
    
    # Certifique-se de que os sets de Google Sheets estejam carregados na sessão
    # Inicialize-os apenas se não existirem ou estiverem vazios
    if 'aguardando_set' not in st.session_state or not st.session_state.aguardando_set:
        st.session_state.aguardando_set = carregar_aguardando_google()
    if 'tombados_set' not in st.session_state or not st.session_state.tombados_set:
        st.session_state.tombados_set = carregar_tombados_google()


# Retrieve data for calculations and display - USAR OS SETS DO SESSION_STATE
df = st.session_state.novo_df
tomb = st.session_state.tomb_df
cpfs_ativos = carregar_cpfs_ativos() # Esta função tem seu próprio cache @st.cache_data
tombados = st.session_state.tombados_set # Usa o set do session_state
aguardando = st.session_state.aguardando_set # Usa o set do session_state

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
    active_contracts_df = filtered_df[
        filtered_df['Número CPF/CNPJ'].isin(active_cpfs)
    ].copy()
    
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
        filtered_df, # Usar filtered_df para garantir que se cruze apenas com os contratos da submodalidade específica
        on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        how='inner'
    )
    num_aguardando = len(merged_aguardando)

    # Tombado count
    tombados_df_temp = pd.DataFrame(list(tombados_set), columns=['Número CPF/CNPJ', 'Número Contrato Crédito'])
    merged_tombados = tombados_df_temp.merge(
        filtered_df, # Usar filtered_df para garantir que se cruze apenas com os contratos da submodalidade específica
        on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        how='inner'
    )
    num_tombado = len(merged_tombados)

    return num_inconsistencias, num_consulta_ativa, num_aguardando, num_tombado, inconsistencias_df, registros_consulta_ativa_df, merged_aguardando, merged_tombados

num_inconsistencias, num_consulta_ativa, num_aguardando, num_tombado, inconsistencias_data, registros_consulta_ativa_data, aguardando_conclusao_data, tombado_data = \
    calculate_counts(filtered_common_df, tomb, cpfs_ativos, tombados, aguardando) # Passa os sets atualizados

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
                        # st.cache_data.clear() # Já é chamado dentro de marcar_cpf_ativo
                        # st.rerun() # Já é chamado dentro de marcar_cpf_ativo
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
            # st.cache_data.clear() # Já é chamado dentro de marcar_aguardando
            # st.rerun() # Já é chamado dentro de marcar_aguardando
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
        temp_df['Aguardando'] = temp_df['Contrato_Tuple'].isin(aguardando) # Use o set 'aguardando' atualizado

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
    # Forçar recarregamento atualizado
    st.session_state['aguardando_set'] = carregar_aguardando_google()
    st.session_state['tombados_set'] = carregar_tombados_google()

    # Atualizar dados filtrados com base nos novos tombados
    aguardando_df = pd.DataFrame(list(st.session_state['aguardando_set']), columns=['Número CPF/CNPJ', 'Número Contrato Crédito'])
    aguardando_df['temp_key'] = list(zip(aguardando_df['Número CPF/CNPJ'], aguardando_df['Número Contrato Crédito']))
    aguardando_df = aguardando_df[~aguardando_df['temp_key'].isin(st.session_state['tombados_set'])].drop(columns=['temp_key'])

    merged_aguardando = aguardando_df.merge(
        filtered_common_df,
        on=['Número CPF/CNPJ', 'Número Contrato Crédito'],
        how='inner'
    )

    st.title(f"⏳ Registros Aguardando Conclusão ({len(merged_aguardando)})")

    if not merged_aguardando.empty:
        st.dataframe(merged_aguardando, use_container_width=True)

        unique_cpfs_ag = merged_aguardando['Número CPF/CNPJ'].unique().tolist()
        cpf_escolhido = st.selectbox("CPF para tombar", unique_cpfs_ag, key="cpf_ag_key")

        contratos_filtrados = merged_aguardando[
            merged_aguardando['Número CPF/CNPJ'] == cpf_escolhido
        ]['Número Contrato Crédito'].astype(str).tolist()

        contrato_escolhido = st.selectbox("Contrato para tombar:", contratos_filtrados, key=f"contrato_ag_{cpf_escolhido}")

        if st.button("Marcar como Tombado", key=f"btn_ag_{cpf_escolhido}_{contrato_escolhido}"):
            marcar_tombado(cpf_escolhido, contrato_escolhido)
            st.success(f"Contrato {contrato_escolhido} do CPF {cpf_escolhido} foi tombado com sucesso.")
    else:
        st.info("Nenhum registro encontrado.")


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
