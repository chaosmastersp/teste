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
        # Normaliza CPF ao carregar
        return [str(row[0]).strip().zfill(11) for row in values[1:]]  # Ignora cabeçalho
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
        return set((str(row[0]).strip().zfill(11), str(row[1]).strip()) for row in values[1:])  # (cpf, contrato)
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
        return set((str(row[0]).strip().zfill(11), str(row[1]).strip()) for row in values[1:])
    except Exception as e:
        st.error(f"Erro ao carregar registros aguardando: {e}")
        return set()

# Functions that modify Google Sheets should not be cached, but their calls should invalidate relevant caches
def marcar_tombado(cpf, contrato):
    consulta = client.open("consulta_ativa")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Normaliza CPF e Contrato de entrada IMEDIATAMENTE
    cpf_normalizado = str(cpf).strip().zfill(11)
    contrato_normalizado = str(contrato).strip()

    st.info(f"DEBUG: Tentando tombar CPF: '{cpf_normalizado}', Contrato: '{contrato_normalizado}'") # Mensagem de depuração

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
        
    tomb_sheet.append_row([cpf_normalizado, contrato_normalizado, timestamp])
    st.success(f"DEBUG: Adicionado '{contrato_normalizado}' do CPF '{cpf_normalizado}' à planilha 'tombados'.") # Mensagem de depuração

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
        
        for i, row in enumerate(data):
            # Normaliza os valores da linha do sheet para comparação
            # Garante que a linha tem pelo menos 2 elementos antes de tentar acessar
            current_cpf_in_sheet = str(row[0]).strip().zfill(11) if len(row) > 0 else ""
            current_contrato_in_sheet = str(row[1]).strip() if len(row) > 1 else ""
            
            st.info(f"DEBUG: Comparando sheet row {i+2} (CPF: '{current_cpf_in_sheet}', Contrato: '{current_contrato_in_sheet}') com target (CPF: '{cpf_normalizado}', Contrato: '{contrato_normalizado}')")

            if current_cpf_in_sheet == cpf_normalizado and current_contrato_in_sheet == contrato_normalizado:
                st.info(f"DEBUG: Correspondência encontrada para remover na linha {i+2}: CPF '{current_cpf_in_sheet}', Contrato '{current_contrato_in_sheet}'")
                found_and_removed = True
            else:
                # Mantém a linha original, sem normalizar, para reescrita
                new_data.append(row)
        
        if not found_and_removed:
            st.warning(f"DEBUG: Contrato (CPF: '{cpf_normalizado}', Contrato: '{contrato_normalizado}') NÃO encontrado na planilha 'aguardando' para remoção. Verifique os valores na planilha e no input.")

        st.info(f"DEBUG: Dados restantes em 'aguardando' após a tentativa de remoção: {new_data}")

        # Recria a planilha com cabeçalho + dados válidos
        values_to_update = [header] + new_data
        
        st.info("DEBUG: Tentando aguard_sheet.clear()")
        aguard_sheet.clear() # Clear the entire sheet
        st.info("DEBUG: aguard_sheet.clear() concluído.")
        
        if values_to_update and len(values_to_update) > 0: # Ensure header is always written, and data if present
            st.info(f"DEBUG: Conteúdo a ser atualizado em 'aguardando': {values_to_update}")
            st.info(f"DEBUG: Tentando aguard_sheet.update('A1', {len(values_to_update)} linhas)")
            aguard_sheet.update("A1", values_to_update)
            st.info("DEBUG: aguard_sheet.update() concluído.")
            st.success(f"DEBUG: Contrato '{contrato_normalizado}' do CPF '{cpf_normalizado}' removido da planilha 'aguardando' com sucesso.")
        else: # This case should ideally not be hit if header is always included, but as a safeguard
            st.warning("DEBUG: `values_to_update` estava vazio ou apenas com cabeçalho. Planilha 'aguardando' pode estar vazia.")
            aguard_sheet.append_row(header) # Ensure header is present if nothing else is
            st.success(f"DEBUG: Contrato '{contrato_normalizado}' do CPF '{cpf_normalizado}' removido. Planilha 'aguardando' agora contém apenas o cabeçalho.")
            
        # --- Verificação Pós-Remoção ---
        st.info("DEBUG: Verificando se o registro foi realmente removido do Google Sheet...")
        # Força uma nova conexão para garantir que não há cache do gspread
        client_recheck = get_gspread_client() 
        aguard_sheet_reloaded = client_recheck.open("consulta_ativa").worksheet("aguardando")
        reloaded_values = aguard_sheet_reloaded.get_all_values()
        
        # Normaliza os valores recarregados para verificação
        reloaded_set = set()
        if len(reloaded_values) > 1:
            for row in reloaded_values[1:]:
                if len(row) >= 2: # Garante que a linha tem pelo menos 2 elementos
                    reloaded_set.add((str(row[0]).strip().zfill(11), str(row[1]).strip()))

        if (cpf_normalizado, contrato_normalizado) not in reloaded_set:
            st.success(f"DEBUG: Confirmação: Registro (CPF: '{cpf_normalizado}', Contrato: '{contrato_normalizado}') NÃO encontrado na planilha 'aguardando' após a remoção.")
        else:
            st.error(f"DEBUG: ALERTA: Registro (CPF: '{cpf_normalizado}', Contrato: '{contrato_normalizado}') AINDA ENCONTRADO na planilha 'aguardando' após a remoção. Isso é um problema crítico. Verifique manualmente a planilha e as permissões.")
            st.info(f"DEBUG: Conteúdo atual da planilha 'aguardando' após recarga: {reloaded_values}")


    except Exception as e:
        st.error(f"ERRO CRÍTICO ao remover de 'aguardando': {e}")
        st.exception(e) # Show full traceback for better debugging

    st.cache_data.clear()
    st.session_state['aguardando_set'] = carregar_aguardando_google()
    st.session_state['tombados_set'] = carregar_tombados_google()
    st.rerun()


