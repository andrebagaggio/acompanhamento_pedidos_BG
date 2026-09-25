"""
Acompanhamento Gráfico - Infor WMS
-----------------------------------
App Streamlit para consultar uma faixa de pedidos no Infor WMS
via API, exibir indicadores e gráficos de status.

Modo mais on time possivel, posso usar na bag tmb
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import streamlit as st
import plotly.express as px

# =========================================================
# CONFIGURAÇÕES / CREDENCIAIS (fixas no código)
# =========================================================
CLIENT_ID = st.secrets["CI"]
CLIENT_SECRET = st.secrets["CS"]
USERNAME = st.secrets["USERNAME"]
PASSWORD = st.secrets["PASSWORD"]

TOKEN_URL = st.secrets["TOKEN"]
WHSE_BASE_URL = (
    "https://mingle-ionapi.inforcloudsuite.com/US45PBYRE7XKA5QB_PRD/WM/wmwebservice_rest/"
    "US45PBYRE7XKA5QB_PRD_COBALTLIKABLECAT_PRD_SCE_PRD_4_wmwhse1"
)
BASE_URL = f"{WHSE_BASE_URL}/shipments"
TASKS_URL = f"{WHSE_BASE_URL}/tasks/list"

# A partir deste status (inclusive) o pedido já possui tarefas geradas no WMS
STATUS_MIN_TAREFAS = 29

# Código de status de tarefa considerado "Concluído" (para o ranking de colaboradores)
TASK_STATUS_CONCLUIDO = "9"

# Colunas do dataframe de tarefas (usado em vários pontos para inicializar dataframe vazio)
COLUNAS_TAREFAS = [
    "Pedido", "StatusCod", "Status", "TipoTarefa", "SKU", "Qtd",
    "DeLoc", "ParaLoc", "UserKey", "StartTime", "EndTime", "ReasonCod",
]

# Tradução dos códigos de status do WMS
STATUS_MAP = {
    "00": "Ordem em branco",
    "02": "Criado extern.",
    "04": "Criado intern.",
    "06": "Não alocou",
    "08": "Convertido",
    "09": "Não inic.",
    "-1": "Desc.",
    "10": "Agrupado",
    "11": "Volume pré-alocado",
    "12": "Pré-alocado",
    "13": "Liberado p/ planej. de armaz.",
    "14": "Volume alocado",
    "15": "Volume aloc./volume sep.",
    "16": "Volume alocado/volume exp.",
    "17": "Alocado",
    "18": "Substituído",
    "-2": "SemSincronismo",
    "22": "Volume liberado",
    "25": "Volume liberado/volume sep.",
    "27": "Volume liberado/volume exp.",
    "29": "Liberado",
    "51": "Em separação",
    "52": "Vol. sep.",
    "53": "Vol. separado/volume exp.",
    "55": "Separação concluída",
    "57": "Separado/volume exp.",
    "61": "Em emb.",
    "68": "Emb. concluída",
    "75": "Preparado",
    "78": "Manifestado",
    "82": "Em carreg.",
    "88": "Carregado",
    "92": "Volume expedido",
    "94": "Fechar produção",
    "95": "Expedição concluída",
    "96": "Entrega aceita",
    "97": "Entrega recusada",
    "98": "Cancelado extern.",
    "99": "Cancelado intern.",
}


def traduzir_status(codigo) -> str:
    """Traduz o código de status do pedido (shipment) para a descrição correspondente."""
    return STATUS_MAP.get(str(codigo).strip(), f"Desconhecido ({codigo})")


# Tradução dos códigos de status das TAREFAS (tasks/list)
TASK_STATUS_MAP = {
    "0": "Pendente",
    "3": "Em processamento",
    "8": "Aguardando sequência",
    "9": "Concluído",
    "H": "Bloqueado pelo usuário",
    "R": "Rejeitado",
    "S": "Bloqueado pelo sistema",
    "X": "Cancelado",
}


def traduzir_status_tarefa(codigo) -> str:
    """Traduz o código de status da tarefa para a descrição correspondente."""
    return TASK_STATUS_MAP.get(str(codigo).strip(), f"Desconhecido ({codigo})")


st.set_page_config(
    page_title="Acompanhamento Demanda",
    page_icon="📦",
    layout="wide",
)


# =========================================================
# FUNÇÕES DE APOIO
# =========================================================
@st.cache_data(ttl=1500, show_spinner=False)
def get_token() -> str:
    """Obtém (e mantém em cache por ~25min) o token OAuth2."""
    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
    }
    response = requests.post(TOKEN_URL, data=payload, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def consulta_wms(pedido: int, token: str) -> dict:
    """Consulta um único pedido (shipment) no WMS."""
    url = f"{BASE_URL}/{pedido}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_lista_or(texto: str) -> list[str]:
    """
    Converte uma string como 'pedido1 or pedido2 OR pedido3' em uma lista de pedidos.
    A separação por 'or' não diferencia maiúsculas/minúsculas (or, OR, Or, oR...).
    """
    if not texto or not texto.strip():
        return []
    partes = re.split(r"\s*\bor\b\s*", texto.strip(), flags=re.IGNORECASE)
    return [p.strip() for p in partes if p.strip()]


def consultar_pedidos(lista_pedidos: list, token: str, max_workers: int = 10) -> tuple[list[dict], list]:
    """
    Consulta uma lista de pedidos (faixa ou lista avulsa) em paralelo, via thread pool.
    Como o gargalo é a latência de rede (I/O), paralelizar acelera bastante sem pesar
    no processamento — cada consulta HTTP roda em sua própria thread.
    Erros pontuais não interrompem as demais consultas.
    """
    resultados, falhas = [], []
    total = len(lista_pedidos)
    concluidos = 0
    barra = st.progress(0.0, text="Consultando pedidos...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {executor.submit(consulta_wms, pedido, token): pedido for pedido in lista_pedidos}

        for futuro in as_completed(futuros):
            pedido = futuros[futuro]
            try:
                dados = futuro.result()
                codigo_status = dados.get("status", "")
                resultados.append(
                    {
                        "Pedido": dados.get("orderkey", pedido),
                        "StatusCod": codigo_status,
                        "Status": traduzir_status(codigo_status),
                        "Peças": dados.get("totalqty", 0),
                        "MensagemNota": dados.get("ext_udf_str4", ""),
                    }
                )
            except requests.exceptions.RequestException:
                falhas.append(pedido)

            concluidos += 1
            barra.progress(concluidos / total, text=f"Consultando pedidos... ({concluidos}/{total})")

    barra.empty()
    return resultados, falhas


def consulta_tasks(pedido, token) -> list:
    """Consulta as tarefas geradas (WMS) para um pedido, via POST."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(TASKS_URL, headers=headers, json={"orderkey": str(pedido)}, timeout=30)
    response.raise_for_status()
    dados = response.json()
    return dados if isinstance(dados, list) else []


def pedido_elegivel_tarefas(codigo_status) -> bool:
    """Verifica se o pedido já está em um status (>= STATUS_MIN_TAREFAS) que gera tarefas no WMS."""
    try:
        return int(str(codigo_status).strip()) >= STATUS_MIN_TAREFAS
    except ValueError:
        return False


def consultar_tarefas(lista_pedidos: list, token: str, max_workers: int = 10) -> tuple[list[dict], list]:
    """
    Consulta, em paralelo, as tarefas geradas (endpoint tasks/list) para uma lista de pedidos.
    Retorna uma lista "achatada" com uma linha por tarefa (já com o status traduzido),
    incluindo usuário responsável, horários de início/fim, motivo (reasonkey) e a
    posição de origem (fromloc) — usados no ranking de colaboradores, no ranking de
    motivos e na classificação de pedidos por localização.
    """
    tarefas_flat, falhas = [], []
    total = len(lista_pedidos)
    if total == 0:
        return tarefas_flat, falhas

    concluidos = 0
    barra = st.progress(0.0, text="Consultando tarefas geradas...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {executor.submit(consulta_tasks, pedido, token): pedido for pedido in lista_pedidos}

        for futuro in as_completed(futuros):
            pedido = futuros[futuro]
            try:
                tarefas = futuro.result()
                for tarefa in tarefas:
                    codigo_status = tarefa.get("status", "")
                    tarefas_flat.append(
                        {
                            "Pedido": pedido,
                            "StatusCod": codigo_status,
                            "Status": traduzir_status_tarefa(codigo_status),
                            "TipoTarefa": tarefa.get("tasktype", ""),
                            "SKU": tarefa.get("sku", ""),
                            "Qtd": tarefa.get("qty", 0),
                            "DeLoc": tarefa.get("fromloc", ""),
                            "ParaLoc": tarefa.get("toloc", ""),
                            "UserKey": tarefa.get("userkey", ""),
                            "StartTime": tarefa.get("starttime"),
                            "EndTime": tarefa.get("endtime"),
                            "ReasonCod": tarefa.get("reasonkey", ""),
                        }
                    )
            except requests.exceptions.RequestException:
                falhas.append(pedido)

            concluidos += 1
            barra.progress(concluidos / total, text=f"Consultando tarefas geradas... ({concluidos}/{total})")

    barra.empty()
    return tarefas_flat, falhas


def calcular_ranking_colaboradores(df_tarefas: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    A partir das tarefas com status Concluído (e sem motivo/reasoncode preenchido — essas
    são desconsideradas do ranking), monta o ranking dos colaboradores: quantidade de
    tarefas separadas, tempo médio de separação (EndTime - StartTime) e a média de
    peças separadas por hora (soma de Qtd / soma de horas trabalhadas).
    """
    colunas_saida = ["Usuário", "Tarefas Concluídas", "Tempo Médio (min)", "Peças/Hora"]
    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas[df_tarefas["StatusCod"].astype(str).str.strip() == TASK_STATUS_CONCLUIDO].copy()
    df = df[df["UserKey"].astype(str).str.strip() != ""]
    df = df[df["ReasonCod"].astype(str).str.strip() == ""]  # desconsidera tarefas com motivo preenchido
    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce", utc=True)
    df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce", utc=True)
    df["DuracaoMin"] = (df["EndTime"] - df["StartTime"]).dt.total_seconds() / 60
    df = df[(df["DuracaoMin"] >= 0) & df["DuracaoMin"].notna()]
    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    resumo = (
        df.groupby("UserKey")
        .agg(
            Tarefas_Concluidas=("UserKey", "count"),
            Tempo_Medio_Min=("DuracaoMin", "mean"),
            Total_Qtd=("Qtd", "sum"),
            Total_Min=("DuracaoMin", "sum"),
        )
        .reset_index()
    )
    resumo["Total_Horas"] = resumo["Total_Min"] / 60
    resumo["Peças/Hora"] = resumo.apply(
        lambda r: round(r["Total_Qtd"] / r["Total_Horas"], 1) if r["Total_Horas"] > 0 else 0,
        axis=1,
    )
    resumo["Tempo Médio (min)"] = resumo["Tempo_Medio_Min"].round(1)
    resumo = resumo.rename(columns={"UserKey": "Usuário", "Tarefas_Concluidas": "Tarefas Concluídas"})
    resumo = resumo[colunas_saida].sort_values("Tarefas Concluídas", ascending=False).head(top_n)
    return resumo.reset_index(drop=True)


def calcular_ranking_motivos(df_tarefas: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Ranking dos motivos (ReasonCod) presentes nas tarefas, por número de ocorrências."""
    colunas_saida = ["Motivo", "Ocorrências"]
    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas[df_tarefas["ReasonCod"].astype(str).str.strip() != ""]
    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    contagem = df["ReasonCod"].value_counts().reset_index()
    contagem.columns = colunas_saida
    return contagem.head(top_n)


def classificar_localizacao(fromloc) -> str:
    """
    Classifica a posição de origem (fromloc) de uma tarefa:
    termina em '000' ou '010' -> Baixo; termina em '020' -> Médio; demais -> Alto.
    """
    loc = str(fromloc).strip()
    if loc.endswith("000") or loc.endswith("010") or loc.endswith("01") or loc.endswith("02") or loc.endswith("03") or loc.endswith("04") or loc.endswith("05") or loc.endswith("06"):
        return "Baixo"
    if loc.endswith("020"):
        return "Médio"
    return "Alto"


def classificar_pedidos_por_localizacao(df_tarefas: pd.DataFrame) -> pd.DataFrame:
    """
    Classifica cada pedido conforme as posições (fromloc) das suas tarefas:
    - Só Alto -> ALTO / Só Baixo -> BAIXO / Só Médio -> MÉDIO
    - Alto + Baixo -> Parcial A/B
    - Médio + Baixo -> Parcial M/B
    - Alto + Médio -> Parcial A/M
    - Alto + Médio + Baixo -> Misto A/M/B
    """
    colunas_saida = ["Pedido", "Classificação"]
    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas.copy()
    df["_Faixa"] = df["DeLoc"].apply(classificar_localizacao)

    mapa_combinacoes = {
        frozenset({"Alto"}): "ALTO",
        frozenset({"Baixo"}): "BAIXO",
        frozenset({"Médio"}): "MÉDIO",
        frozenset({"Alto", "Baixo"}): "Parcial A/B",
        frozenset({"Médio", "Baixo"}): "Parcial M/B",
        frozenset({"Alto", "Médio"}): "Parcial A/M",
        frozenset({"Alto", "Médio", "Baixo"}): "Misto A/M/B",
    }

    resumo = (
        df.groupby("Pedido")["_Faixa"]
        .apply(lambda faixas: mapa_combinacoes[frozenset(faixas)])
        .reset_index()
    )
    resumo.columns = colunas_saida
    return resumo


# =========================================================
# INTERFACE
# =========================================================
st.title("📦 Acompanhamento De Demandas - Infor WMS")
st.caption("Consulta de pedidos (shipments) via API REST do Infor WMS")

with st.sidebar:
    st.header("Filtros de consulta")
    modo_busca = st.radio("Tipo de busca", ["Faixa de pedidos", "Lista de pedidos (OR)"])

    if modo_busca == "Faixa de pedidos":
        inicio = st.number_input("Pedido inicial", min_value=1, step=1)
        fim = st.number_input("Pedido final", min_value=int(inicio), step=1, value=int(inicio))
    else:
        texto_pedidos = st.text_area(
            "Pedidos",
            placeholder="pedido1 or pedido2 or pedido3",
            help="Separe os pedidos com 'or' (não diferencia maiúsculas/minúsculas: or, OR, Or...).",
        )

    paralelismo = st.slider(
        "Consultas simultâneas", min_value=1, max_value=30, value=10,
        help="Número de requisições feitas em paralelo. Valores maiores aceleram a consulta.",
    )

    consultar = st.button("🔍 Consultar", use_container_width=True)

if consultar:
    try:
        if modo_busca == "Faixa de pedidos":
            lista_pedidos = list(range(int(inicio), int(fim) + 1))
        else:
            lista_pedidos = parse_lista_or(texto_pedidos)
            if not lista_pedidos:
                st.warning("Informe ao menos um pedido, separado por 'or'.")
                st.stop()

        with st.spinner("Autenticando..."):
            token = get_token()

        resultados, falhas = consultar_pedidos(lista_pedidos, token, max_workers=paralelismo)

        if not resultados:
            st.warning("Nenhum pedido foi retornado para a faixa informada.")
        else:
            df = pd.DataFrame(resultados)
            st.session_state["df_pedidos"] = df
            st.session_state["falhas"] = falhas

            # Pedidos com status >= 29 já geraram tarefas no WMS
            pedidos_elegiveis = sorted(
                {r["Pedido"] for r in resultados if pedido_elegivel_tarefas(r["StatusCod"])}
            )
            tarefas_flat, tarefas_falhas = consultar_tarefas(
                pedidos_elegiveis, token, max_workers=paralelismo
            )
            st.session_state["df_tarefas"] = (
                pd.DataFrame(tarefas_flat) if tarefas_flat else pd.DataFrame(columns=COLUNAS_TAREFAS)
            )
            st.session_state["tarefas_falhas"] = tarefas_falhas

    except requests.exceptions.HTTPError as e:
        st.error(f"Erro de autenticação/API: {e}")
    except Exception as e:
        st.error(f"Erro inesperado: {e}")

# =========================================================
# RESULTADOS (persistem entre interações, ex.: ao usar o filtro)
# =========================================================
if "df_pedidos" in st.session_state:
    df = st.session_state["df_pedidos"]
    falhas = st.session_state.get("falhas", [])

    if falhas:
        st.warning(f"⚠️ {len(falhas)} pedido(s) não puderam ser consultados: {falhas}")

    # ---- Filtro por MensagemNota ----
    mensagens = ["Todos"] + sorted(df["MensagemNota"].dropna().unique().tolist())
    filtro = st.selectbox("Filtrar por Mensagem/Nota", mensagens)
    df_filtrado = df if filtro == "Todos" else df[df["MensagemNota"] == filtro]

    st.divider()

    # ---- Indicadores (KPIs) ----
    total_pedidos = df_filtrado["Pedido"].nunique()
    total_pecas = int(df_filtrado["Peças"].sum())
    media_pecas = round(df_filtrado["Peças"].mean(), 1) if total_pedidos else 0
    status_predominante = (
        df_filtrado["Status"].mode()[0] if not df_filtrado.empty else "-"
    )

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("📄 Pedidos", total_pedidos)
    col2.metric("📦 Total de Peças", f"{total_pecas:,}".replace(",", "."))
    col3.metric("📊 Média de Peças/Pedido", media_pecas)
    col4.metric("🏷️ Status Predominante", status_predominante)

    df_tarefas = st.session_state.get("df_tarefas", pd.DataFrame(columns=COLUNAS_TAREFAS))
    tarefas_falhas = st.session_state.get("tarefas_falhas", [])
    total_tarefas = len(df_tarefas)
    col5.metric("🗂️ Tarefas Geradas", total_tarefas)

    st.divider()

    # ---- Gráficos ----
    g1, g2 = st.columns(2)

    with g1:
        st.subheader("Peças por Status")
        fig_bar = px.bar(
            df_filtrado.groupby("Status", as_index=False)["Peças"].sum(),
            x="Status",
            y="Peças",
            color="Status",
            text_auto=True,
        )
        fig_bar.update_layout(showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    with g2:
        st.subheader("Distribuição de Pedidos por Status")
        fig_pie = px.pie(
            df_filtrado,
            names="Status",
            hole=0.45,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    if not df_tarefas.empty:
        st.subheader("Tarefas por Status")
        contagem_tarefas = df_tarefas["Status"].value_counts().reset_index()
        contagem_tarefas.columns = ["Status", "Qtd Tarefas"]
        fig_tarefas = px.bar(
            contagem_tarefas,
            x="Status",
            y="Qtd Tarefas",
            color="Status",
            text_auto=True,
        )
        fig_tarefas.update_layout(showlegend=False)
        st.plotly_chart(fig_tarefas, use_container_width=True)

    # ---- Ranking de colaboradores (tarefas concluídas) ----
    ranking = calcular_ranking_colaboradores(df_tarefas)
    if not ranking.empty:
        st.divider()
        st.subheader("🏆 Top 10 Colaboradores - Tarefas Concluídas")
        st.caption("Tarefas com motivo (ReasonCod) preenchido não entram nesse ranking.")

        rc1, rc2 = st.columns(2)

        with rc1:
            fig_ranking = px.bar(
                ranking.sort_values("Tarefas Concluídas"),
                x="Tarefas Concluídas",
                y="Usuário",
                orientation="h",
                text_auto=True,
                title="Tarefas concluídas por colaborador",
            )
            fig_ranking.update_layout(showlegend=False)
            st.plotly_chart(fig_ranking, use_container_width=True)

        with rc2:
            fig_pecas_hora = px.bar(
                ranking.sort_values("Peças/Hora"),
                x="Peças/Hora",
                y="Usuário",
                orientation="h",
                text_auto=True,
                title="Média de peças separadas por hora",
            )
            fig_pecas_hora.update_layout(showlegend=False)
            st.plotly_chart(fig_pecas_hora, use_container_width=True)

        st.dataframe(ranking, use_container_width=True, hide_index=True)
        st.caption(
            "Tempo médio calculado a partir de StartTime/EndTime das tarefas com status "
            f"'{traduzir_status_tarefa(TASK_STATUS_CONCLUIDO)}'. "
            "Peças/Hora = soma de peças separadas ÷ soma de horas trabalhadas pelo colaborador."
        )

    # ---- Ranking de motivos (reasoncode) ----
    ranking_motivos = calcular_ranking_motivos(df_tarefas)
    if not ranking_motivos.empty:
        st.divider()
        st.subheader("🚩 Ranking de Motivos (Reason Code)")
        fig_motivos = px.bar(
            ranking_motivos.sort_values("Ocorrências"),
            x="Ocorrências",
            y="Motivo",
            orientation="h",
            text_auto=True,
        )
        fig_motivos.update_layout(showlegend=False)
        st.plotly_chart(fig_motivos, use_container_width=True)
        st.dataframe(ranking_motivos, use_container_width=True, hide_index=True)

    # ---- Classificação de pedidos por localização (fromloc) ----
    classificacao_pedidos = classificar_pedidos_por_localizacao(df_tarefas)
    if not classificacao_pedidos.empty:
        st.divider()
        st.subheader("📍 Classificação dos Pedidos por Localização")
        st.caption(
            "Baseado no fromloc das tarefas: terminação 000/010 = Baixo, 020 = Médio, demais = Alto. "
            "Pedidos com mais de uma faixa aparecem como parcial/misto."
        )

        cl1, cl2 = st.columns(2)
        with cl1:
            contagem_classificacao = classificacao_pedidos["Classificação"].value_counts().reset_index()
            contagem_classificacao.columns = ["Classificação", "Qtd Pedidos"]
            fig_classificacao = px.bar(
                contagem_classificacao,
                x="Classificação",
                y="Qtd Pedidos",
                color="Classificação",
                text_auto=True,
            )
            fig_classificacao.update_layout(showlegend=False)
            st.plotly_chart(fig_classificacao, use_container_width=True)

        with cl2:
            st.dataframe(
                classificacao_pedidos.sort_values("Pedido"),
                use_container_width=True,
                hide_index=True,
                height=380,
            )

    st.divider()

    # ---- Tabela detalhada ----
    st.subheader("📋 Pedidos")
    st.dataframe(df_filtrado, use_container_width=True, hide_index=True)

    st.download_button(
        "⬇️ Baixar CSV",
        data=df_filtrado.to_csv(index=False).encode("utf-8"),
        file_name="pedidos_wms.csv",
        mime="text/csv",
    )

    if not df_tarefas.empty:
        st.divider()
        st.subheader(f"🗂️ Tarefas Geradas (pedidos com status ≥ {STATUS_MIN_TAREFAS})")
        if tarefas_falhas:
            st.warning(f"⚠️ Não foi possível consultar tarefas de {len(tarefas_falhas)} pedido(s): {tarefas_falhas}")

        resumo_pedido = df_tarefas.groupby("Pedido").size().reset_index(name="Qtd. de Tarefas")
        st.dataframe(resumo_pedido.sort_values("Pedido"), use_container_width=True, hide_index=True)

        with st.expander("Ver detalhamento das tarefas"):
            st.dataframe(df_tarefas, use_container_width=True, hide_index=True)
else:
    st.info("Informe a faixa de pedidos na barra lateral e clique em **Consultar**.")
