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
    st.info(f"DEBUG: Adicionado {contrato} do CPF {cpf} à planilha 'tombados'.") # Mensagem de depuração

    # Remove da planilha 'aguardando'
    try:
        aguard_sheet = consulta.worksheet("aguardando")
        all_values = aguard_sheet.get_all_values()
        
        if not all_values:
            st.warning("DEBUG: Planilha 'aguardando' está vazia, nada para remover.") # Mensagem de depuração
            # Se a planilha estiver vazia (apenas cabeçalho), apenas garanta que o cabeçalho está lá
            try:
                aguard_sheet.clear()
                aguard_sheet.append_row(["cpf", "contrato", "timestamp"])
            except Exception as e_clear:
                st.error(f"Erro ao limpar ou adicionar cabeçalho em 'aguardando': {e_clear}")
            
            st.cache_data.clear()
            st.session_state['aguardando_set'] = carregar_aguardando_google()
            st.session_state['tombados_set'] = carregar_tombados_google()
            st.rerun()
            return

        header = all_values[0]
        data = all_values[1:]
        st.info(f"DEBUG: Dados lidos de 'aguardando' (data): {data}") # Mensagem de depuração
        st.info(f"DEBUG: Tentando remover CPF: {cpf}, Contrato: {contrato}") # Mensagem de depuração

        # Filtra os dados, garantindo que a comparação seja entre strings
        new_data = [row for row in data if not (str(row[0]) == str(cpf) and str(row[1]) == str(contrato))]
        
        st.info(f"DEBUG: Dados restantes após a filtragem: {new_data}") # Mensagem de depuração

        # Recria a planilha com header + dados válidos
        values_to_update = [header] + new_data
        
        aguard_sheet.clear() # Limpa a planilha
        
        # Só atualiza se houver dados (incluindo o cabeçalho)
        if values_to_update:
            aguard_sheet.update("A1", values_to_update)
        else: # Caso todos os dados tenham sido removidos, garante que o cabeçalho permaneça
            aguard_sheet.append_row(header)
        
        st.success(f"DEBUG: Contrato {contrato} do CPF {cpf} removido da planilha 'aguardando' com sucesso.") # Mensagem de depuração

    except Exception as e:
        st.error(f"ERRO CRÍTICO ao remover de 'aguardando': {e}") # Mensagem de erro mais visível

    # Invalidate specific caches related to data modification
    st.cache_data.clear() # Limpa todos os caches de @st.cache_data
    
    # Força a atualização dos sets no session_state para refletir as mudanças imediatamente
    st.session_state['aguardando_set'] = carregar_aguardando_google()
    st.session_state['tombados_set'] = carregar_tombados_google()
    st.rerun() # Força a reexecução do script para que as contagens e a exibição sejam atualizadas
