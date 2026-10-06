import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import pyodbc
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# CONFIGURAÇÕES / CREDENCIAIS
# =========================================================
# API INFOR
CLIENT_ID = st.secrets["CI"]
CLIENT_SECRET = st.secrets["CS"]
USERNAME = st.secrets["USERNAME"]
PASSWORD = st.secrets["PASSWORD"]
TOKEN_URL = st.secrets["TOKEN"]

WHSE_BASE_URL = (
    "https://mingle-ionapi.inforcloudsuite.com/"
    "US45PBYRE7XKA5QB_PRD/WM/wmwebservice_rest/"
    "US45PBYRE7XKA5QB_PRD_COBALTLIKABLECAT_PRD_SCE_PRD_4_wmwhse1"
)

BASE_URL = f"{WHSE_BASE_URL}/shipments"
TASKS_URL = f"{WHSE_BASE_URL}/tasks/list"

# SQL SERVER
SQL_SERVER = st.secrets["SQL_SERVER"]
SQL_DATABASE = st.secrets["SQL_DATABASE"]
SQL_USERNAME = st.secrets["SQL_USERNAME"]
SQL_PASSWORD = st.secrets["SQL_PASSWORD"]
SQL_DRIVER = st.secrets.get("SQL_DRIVER", "ODBC Driver 18 for SQL Server")

# Opcional: se houver certificado interno sem cadeia confiável,
# deixe SQL_TRUST_CERTIFICATE = "yes" no secrets.
SQL_TRUST_CERTIFICATE = str(
    st.secrets.get("SQL_TRUST_CERTIFICATE", "yes")
).strip().lower()

# Data mínima da base, conforme regra atual da consulta.
DATA_MINIMA_SQL = date(2026, 8, 2)


# =========================================================
# LIMITES / CACHE
# =========================================================
MAX_PEDIDOS_POR_CONSULTA = 3000
MAX_CONSULTAS_POR_USUARIO = 15
MAX_REQUISICOES_GLOBAIS = 12

CACHE_TTL_SEGUNDOS = 180
CACHE_MAX_ITENS = 6000

HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT = 45
TOKEN_TIMEOUT = 30

STATUS_MIN_TAREFAS = 29
TASK_STATUS_CONCLUIDO = "9"
FUSO_HORARIO = "America/Sao_Paulo"

COLUNAS_TAREFAS = [
    "Pedido",
    "StatusCod",
    "Status",
    "TipoTarefa",
    "SKU",
    "Qtd",
    "DeLoc",
    "ParaLoc",
    "UserKey",
    "StartTime",
    "EndTime",
    "ReasonCod",
]

COLUNAS_SQL = [
    "PEDIDO",
    "FILIAL",
    "DATA_CRIACAO",
    "MENSAGEM_NOTA",
    "CODIGO",
    "TIPO_ATENDIMENTO",
]


# =========================================================
# STATUS DOS PEDIDOS
# =========================================================
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


def traduzir_status(codigo) -> str:
    return STATUS_MAP.get(
        str(codigo).strip(),
        f"Desconhecido ({codigo})",
    )


def traduzir_status_tarefa(codigo) -> str:
    return TASK_STATUS_MAP.get(
        str(codigo).strip(),
        f"Desconhecido ({codigo})",
    )


# =========================================================
# CONFIGURAÇÃO STREAMLIT
# =========================================================
st.set_page_config(
    page_title="Acompanhamento Demanda",
    page_icon="📦",
    layout="wide",
)

st.title("📦 Acompanhamento de Demandas - Infor WMS")
st.caption(
    "SQL Server define o universo de pedidos e a API do Infor WMS "
    "complementa status, tarefas e produtividade."
)


# =========================================================
# CACHE TTL EM MEMÓRIA
# =========================================================
class CacheTTL:
    def __init__(self, ttl: int, max_itens: int):
        self.ttl = ttl
        self.max_itens = max_itens
        self._dados: dict = {}
        self._lock = threading.Lock()

    def get(self, chave):
        with self._lock:
            item = self._dados.get(chave)

            if item is None:
                return False, None

            expira_em, valor = item

            if expira_em < time.monotonic():
                del self._dados[chave]
                return False, None

            return True, valor

    def set(self, chave, valor):
        with self._lock:
            self._dados.pop(chave, None)
            self._dados[chave] = (
                time.monotonic() + self.ttl,
                valor,
            )

            while len(self._dados) > self.max_itens:
                self._dados.pop(next(iter(self._dados)))


# =========================================================
# HTTP / RECURSOS COMPARTILHADOS
# =========================================================
def criar_sessao() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=4,
        pool_maxsize=MAX_REQUISICOES_GLOBAIS,
    )

    sessao = requests.Session()
    sessao.mount("https://", adapter)

    return sessao


class Recursos:
    def __init__(self):
        self.sessao = criar_sessao()

        self.limitador = threading.BoundedSemaphore(
            MAX_REQUISICOES_GLOBAIS
        )

        self.cache_pedidos = CacheTTL(
            CACHE_TTL_SEGUNDOS,
            CACHE_MAX_ITENS,
        )

        self.cache_tarefas = CacheTTL(
            CACHE_TTL_SEGUNDOS,
            CACHE_MAX_ITENS,
        )


@st.cache_resource
def get_recursos() -> Recursos:
    return Recursos()


# =========================================================
# SQL SERVER
# =========================================================
def get_sql_connection_string() -> str:
    trust_value = (
        "yes"
        if SQL_TRUST_CERTIFICATE in {"yes", "true", "1", "sim"}
        else "no"
    )

    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;"
        f"TrustServerCertificate={trust_value};"
        "Connection Timeout=30;"
    )


@st.cache_data(ttl=120, show_spinner=False)
def carregar_base_sql(
    data_inicial: date,
    data_final: date,
) -> pd.DataFrame:
    """
    Consulta a SC5010 diretamente no SQL Server.

    O filtro de data é executado no SQL para evitar carregar
    pedidos desnecessários no Streamlit.
    """
    sql = """
    SELECT
        RTRIM(C5_NUM) AS PEDIDO,
        RTRIM(C5_FILIAL) AS FILIAL,
        C5_EMISSAO AS DATA_CRIACAO,
        RTRIM(C5_MENNOTA) AS MENSAGEM_NOTA,

        CASE
            WHEN LEFT(LTRIM(RTRIM(C5_MENNOTA)), 10) LIKE
                 '[0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9]'
            THEN LEFT(LTRIM(RTRIM(C5_MENNOTA)), 10)
            ELSE 'Sem código'
        END AS CODIGO,

        CASE
            WHEN C5_XXA2B = '101' THEN 'PICKING'
            WHEN C5_XXA2B = '202' THEN 'CROSS'
            WHEN C5_XXA2B = '303' THEN 'MKT/BONIFICACAO/AMOSTRA'
            WHEN C5_XXA2B = '404' THEN 'CORPORATIVO'
            WHEN C5_XXA2B = '505' THEN 'ATACADO'
            WHEN C5_XXA2B = '606' THEN 'FATURAMENTO'
            WHEN C5_XXA2B = '707' THEN 'FRANQUIA'
            WHEN C5_XXA2B = '808' THEN 'ENXOVAL'
            WHEN C5_XXA2B = '909' THEN 'ALMOXARIFADO'
            ELSE 'REGISTRAR'
        END AS TIPO_ATENDIMENTO

    FROM SC5010 WITH (NOLOCK)

    WHERE C5_FILIAL IN ('011004', '011005', '011324')
      AND C5_EMISSAO > '20260801'
      AND C5_XXA2B IN (
          '101','202','303','404','505',
          '606','707','808','909'
      )
      AND C5_EMISSAO >= ?
      AND C5_EMISSAO <= ?
      AND D_E_L_E_T_ = ''
    """

    ini_sql = data_inicial.strftime("%Y%m%d")
    fim_sql = data_final.strftime("%Y%m%d")

    with pyodbc.connect(get_sql_connection_string()) as conexao:
        df = pd.read_sql_query(
            sql,
            conexao,
            params=[ini_sql, fim_sql],
        )

    if df.empty:
        return pd.DataFrame(columns=COLUNAS_SQL)

    for coluna in (
        "PEDIDO",
        "FILIAL",
        "MENSAGEM_NOTA",
        "CODIGO",
        "TIPO_ATENDIMENTO",
    ):
        if coluna in df.columns:
            df[coluna] = (
                df[coluna]
                .fillna("")
                .astype(str)
                .str.strip()
            )

    df["DATA_CRIACAO"] = pd.to_datetime(
        df["DATA_CRIACAO"],
        format="%Y%m%d",
        errors="coerce",
    )

    return df


def aplicar_filtros_sql(
    df_sql: pd.DataFrame,
    filiais: list[str],
    tipos: list[str],
    codigos: list[str],
) -> pd.DataFrame:
    df = df_sql.copy()

    if filiais:
        df = df[df["FILIAL"].isin(filiais)]

    if tipos:
        df = df[df["TIPO_ATENDIMENTO"].isin(tipos)]

    if codigos:
        df = df[df["CODIGO"].isin(codigos)]

    return df


# =========================================================
# TOKEN
# =========================================================
@st.cache_data(ttl=3300, show_spinner=False)
def get_token() -> str:
    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
    }

    response = requests.post(
        TOKEN_URL,
        data=payload,
        timeout=TOKEN_TIMEOUT,
    )

    response.raise_for_status()

    dados = response.json()
    token = dados.get("access_token")

    if not token:
        raise requests.exceptions.HTTPError(
            "O endpoint de autenticação não retornou access_token."
        )

    return token


# =========================================================
# REQUISIÇÃO HTTP
# =========================================================
def _requisitar(
    recursos: Recursos,
    metodo: str,
    url: str,
    token: str,
    renovar_token_em_401: bool = True,
    **kwargs,
) -> requests.Response:
    timeout = kwargs.pop(
        "timeout",
        (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
    )

    def enviar(token_atual: str):
        headers = {
            "Authorization": f"Bearer {token_atual}"
        }

        with recursos.limitador:
            return recursos.sessao.request(
                metodo,
                url,
                headers=headers,
                timeout=timeout,
                **kwargs,
            )

    resposta = enviar(token)

    if resposta.status_code == 401 and renovar_token_em_401:
        try:
            get_token.clear()
            novo_token = get_token()
            resposta = enviar(novo_token)
        except Exception:
            pass

    return resposta


# =========================================================
# CONSULTA DE PEDIDO
# =========================================================
def consulta_wms(
    pedido,
    token: str,
    recursos: Recursos,
) -> dict | None:
    chave = str(pedido).strip()

    achou, valor = recursos.cache_pedidos.get(chave)

    if achou:
        return valor

    resposta = _requisitar(
        recursos,
        "GET",
        f"{BASE_URL}/{chave}",
        token,
    )

    if resposta.status_code == 404:
        valor = None
    else:
        resposta.raise_for_status()
        dados = resposta.json()

        valor = {
            "orderkey": dados.get("orderkey", chave),
            "status": dados.get("status", ""),
            "totalqty": dados.get("totalqty", 0),
            "ext_udf_str4": dados.get("ext_udf_str4", ""),
        }

    recursos.cache_pedidos.set(chave, valor)

    return valor


# =========================================================
# CONSULTA DE TAREFAS
# =========================================================
def consulta_tasks(
    pedido,
    token: str,
    recursos: Recursos,
) -> list[dict]:
    chave = str(pedido).strip()

    achou, valor = recursos.cache_tarefas.get(chave)

    if achou:
        return valor

    resposta = _requisitar(
        recursos,
        "POST",
        TASKS_URL,
        token,
        json={"orderkey": chave},
    )

    resposta.raise_for_status()

    dados = resposta.json()
    dados = dados if isinstance(dados, list) else []

    valor = [
        {
            "status": t.get("status", ""),
            "tasktype": t.get("tasktype", ""),
            "sku": t.get("sku", ""),
            "qty": t.get("qty", 0),
            "fromloc": t.get("fromloc", ""),
            "toloc": t.get("toloc", ""),
            "userkey": t.get("userkey", ""),
            "starttime": t.get("starttime"),
            "endtime": t.get("endtime"),
            "reasonkey": t.get("reasonkey", ""),
        }
        for t in dados
    ]

    recursos.cache_tarefas.set(chave, valor)

    return valor


# =========================================================
# PARSER DE LISTA OR
# =========================================================
def parse_lista_or(texto: str) -> list[str]:
    if not texto or not texto.strip():
        return []

    partes = re.split(
        r"\s*\bor\b\s*",
        texto.strip(),
        flags=re.IGNORECASE,
    )

    return list(
        dict.fromkeys(
            p.strip()
            for p in partes
            if p.strip()
        )
    )


# =========================================================
# CONSULTAR PEDIDOS
# =========================================================
def consultar_pedidos(
    lista_pedidos: list,
    token: str,
    recursos: Recursos,
    max_workers: int = 8,
) -> tuple[list[dict], list, list]:
    resultados = []
    nao_encontrados = []
    falhas = []

    lista_pedidos = list(
        dict.fromkeys(
            str(p).strip()
            for p in lista_pedidos
            if str(p).strip()
        )
    )

    total = len(lista_pedidos)

    if total == 0:
        return resultados, nao_encontrados, falhas

    max_workers = max(
        1,
        min(
            int(max_workers),
            MAX_CONSULTAS_POR_USUARIO,
            MAX_REQUISICOES_GLOBAIS,
        ),
    )

    concluidos = 0

    barra = st.progress(
        0.0,
        text="Consultando pedidos na API do Infor...",
    )

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        futuros = {
            executor.submit(
                consulta_wms,
                pedido,
                token,
                recursos,
            ): pedido
            for pedido in lista_pedidos
        }

        for futuro in as_completed(futuros):
            pedido = futuros[futuro]

            try:
                dados = futuro.result()

                if dados is None:
                    nao_encontrados.append(pedido)
                else:
                    codigo_status = dados["status"]

                    resultados.append(
                        {
                            "Pedido": str(
                                dados["orderkey"]
                            ).strip(),
                            "StatusCod": codigo_status,
                            "Status": traduzir_status(
                                codigo_status
                            ),
                            "Peças": pd.to_numeric(
                                dados["totalqty"],
                                errors="coerce",
                            ),
                            "MensagemNotaAPI": dados[
                                "ext_udf_str4"
                            ],
                        }
                    )

            except requests.exceptions.RequestException:
                falhas.append(pedido)

            concluidos += 1

            barra.progress(
                concluidos / total,
                text=(
                    "Consultando pedidos na API do Infor... "
                    f"({concluidos}/{total})"
                ),
            )

    barra.empty()

    return resultados, nao_encontrados, falhas


# =========================================================
# ELEGIBILIDADE DE TAREFAS
# =========================================================
def pedido_elegivel_tarefas(codigo_status) -> bool:
    try:
        return (
            int(str(codigo_status).strip())
            >= STATUS_MIN_TAREFAS
        )
    except ValueError:
        return False


# =========================================================
# CONSULTAR TAREFAS
# =========================================================
def consultar_tarefas(
    lista_pedidos: list,
    token: str,
    recursos: Recursos,
    max_workers: int = 8,
) -> tuple[list[dict], list]:
    tarefas_flat = []
    falhas = []

    lista_pedidos = list(
        dict.fromkeys(
            str(p).strip()
            for p in lista_pedidos
            if str(p).strip()
        )
    )

    total = len(lista_pedidos)

    if total == 0:
        return tarefas_flat, falhas

    max_workers = max(
        1,
        min(
            int(max_workers),
            MAX_CONSULTAS_POR_USUARIO,
            MAX_REQUISICOES_GLOBAIS,
        ),
    )

    concluidos = 0

    barra = st.progress(
        0.0,
        text="Consultando tarefas geradas...",
    )

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        futuros = {
            executor.submit(
                consulta_tasks,
                pedido,
                token,
                recursos,
            ): pedido
            for pedido in lista_pedidos
        }

        for futuro in as_completed(futuros):
            pedido = futuros[futuro]

            try:
                for tarefa in futuro.result():
                    codigo_status = tarefa["status"]

                    tarefas_flat.append(
                        {
                            "Pedido": str(pedido).strip(),
                            "StatusCod": codigo_status,
                            "Status": traduzir_status_tarefa(
                                codigo_status
                            ),
                            "TipoTarefa": tarefa["tasktype"],
                            "SKU": tarefa["sku"],
                            "Qtd": pd.to_numeric(
                                tarefa["qty"],
                                errors="coerce",
                            ),
                            "DeLoc": tarefa["fromloc"],
                            "ParaLoc": tarefa["toloc"],
                            "UserKey": tarefa["userkey"],
                            "StartTime": tarefa["starttime"],
                            "EndTime": tarefa["endtime"],
                            "ReasonCod": tarefa["reasonkey"],
                        }
                    )

            except requests.exceptions.RequestException:
                falhas.append(pedido)

            concluidos += 1

            barra.progress(
                concluidos / total,
                text=(
                    "Consultando tarefas geradas... "
                    f"({concluidos}/{total})"
                ),
            )

    barra.empty()

    return tarefas_flat, falhas


# =========================================================
# RANKING DE COLABORADORES
# =========================================================
def calcular_ranking_colaboradores(
    df_tarefas: pd.DataFrame,
    top_n: int = 10,
) -> pd.DataFrame:
    colunas_saida = [
        "Usuário",
        "Tarefas Concluídas",
        "Tempo Médio (min)",
        "Peças/Hora",
    ]

    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas[
        df_tarefas["StatusCod"]
        .astype(str)
        .str.strip()
        == TASK_STATUS_CONCLUIDO
    ].copy()

    df = df[
        df["UserKey"]
        .astype(str)
        .str.strip()
        != ""
    ]

    # Tarefas com motivo não entram na produtividade.
    df = df[
        df["ReasonCod"]
        .astype(str)
        .str.strip()
        == ""
    ]

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["Qtd"] = pd.to_numeric(
        df["Qtd"],
        errors="coerce",
    ).fillna(0)

    df["StartTime"] = pd.to_datetime(
        df["StartTime"],
        errors="coerce",
        utc=True,
    )

    df["EndTime"] = pd.to_datetime(
        df["EndTime"],
        errors="coerce",
        utc=True,
    )

    df["DuracaoMin"] = (
        df["EndTime"]
        - df["StartTime"]
    ).dt.total_seconds() / 60

    df = df[
        (df["DuracaoMin"] >= 0)
        & df["DuracaoMin"].notna()
    ]

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
        lambda r: (
            round(
                r["Total_Qtd"] / r["Total_Horas"],
                1,
            )
            if r["Total_Horas"] > 0
            else 0
        ),
        axis=1,
    )

    resumo["Tempo Médio (min)"] = (
        resumo["Tempo_Medio_Min"].round(1)
    )

    resumo = resumo.rename(
        columns={
            "UserKey": "Usuário",
            "Tarefas_Concluidas": "Tarefas Concluídas",
        }
    )

    resumo = (
        resumo[colunas_saida]
        .sort_values(
            "Tarefas Concluídas",
            ascending=False,
        )
        .head(top_n)
    )

    return resumo.reset_index(drop=True)


# =========================================================
# RANKING DE MOTIVOS
# =========================================================
def calcular_ranking_motivos(
    df_tarefas: pd.DataFrame,
    top_n: int = 10,
) -> pd.DataFrame:
    colunas_saida = [
        "Motivo",
        "Ocorrências",
    ]

    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas[
        df_tarefas["ReasonCod"]
        .astype(str)
        .str.strip()
        != ""
    ]

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    contagem = (
        df["ReasonCod"]
        .value_counts()
        .reset_index()
    )

    contagem.columns = colunas_saida

    return contagem.head(top_n)


# =========================================================
# CLASSIFICAÇÃO DE LOCALIZAÇÃO
# =========================================================
def classificar_localizacao(fromloc) -> str:
    loc = str(fromloc).strip()

    if loc.endswith("000") or loc.endswith("010"):
        return "Baixo"

    if loc.endswith("020"):
        return "Médio"

    return "Alto"


def classificar_pedidos_por_localizacao(
    df_tarefas: pd.DataFrame,
) -> pd.DataFrame:
    colunas_saida = [
        "Pedido",
        "Classificação",
    ]

    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas.copy()

    df["_Faixa"] = df["DeLoc"].apply(
        classificar_localizacao
    )

    mapa_combinacoes = {
        frozenset({"Alto"}): "ALTO",
        frozenset({"Baixo"}): "BAIXO",
        frozenset({"Médio"}): "MÉDIO",
        frozenset({"Alto", "Baixo"}): "Parcial A/B",
        frozenset({"Médio", "Baixo"}): "Parcial M/B",
        frozenset({"Alto", "Médio"}): "Parcial A/M",
        frozenset(
            {"Alto", "Médio", "Baixo"}
        ): "Misto A/M/B",
    }

    resumo = (
        df.groupby("Pedido")["_Faixa"]
        .apply(
            lambda faixas: mapa_combinacoes.get(
                frozenset(faixas),
                "Misto",
            )
        )
        .reset_index()
    )

    resumo.columns = colunas_saida

    return resumo


# =========================================================
# ACOMPANHAMENTO HORA A HORA
# =========================================================
def calcular_tarefas_hora_a_hora(
    df_tarefas: pd.DataFrame,
) -> pd.DataFrame:
    colunas_saida = [
        "Data",
        "HoraOrdenacao",
        "Hora",
        "Tarefas Concluídas",
        "Peças",
    ]

    if df_tarefas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_tarefas[
        df_tarefas["StatusCod"]
        .astype(str)
        .str.strip()
        == TASK_STATUS_CONCLUIDO
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["Qtd"] = pd.to_numeric(
        df["Qtd"],
        errors="coerce",
    ).fillna(0)

    df["EndTime"] = pd.to_datetime(
        df["EndTime"],
        errors="coerce",
        utc=True,
    )

    df = df[df["EndTime"].notna()].copy()

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["EndTime"] = df["EndTime"].dt.tz_convert(
        FUSO_HORARIO
    )

    df["Data"] = df["EndTime"].dt.date
    df["HoraOrdenacao"] = df["EndTime"].dt.floor("h")

    resumo = (
        df.groupby(["Data", "HoraOrdenacao"])
        .agg(
            **{
                "Tarefas Concluídas": (
                    "HoraOrdenacao",
                    "count",
                ),
                "Peças": ("Qtd", "sum"),
            }
        )
        .reset_index()
        .sort_values("HoraOrdenacao")
    )

    resumo["Hora"] = (
        resumo["HoraOrdenacao"]
        .dt.strftime("%Hh")
    )

    return resumo[colunas_saida]


# =========================================================
# ESTADO / LIMPEZA
# =========================================================
def limpar_resultados():
    for chave in (
        "df_pedidos",
        "df_sql_selecionado",
        "falhas",
        "nao_encontrados",
        "df_tarefas",
        "tarefas_falhas",
        "origem_consulta",
    ):
        st.session_state.pop(chave, None)


def salvar_resultados(
    resultados: list[dict],
    nao_encontrados: list,
    falhas: list,
    tarefas_flat: list[dict],
    tarefas_falhas: list,
    origem_consulta: str,
    df_sql_selecionado: pd.DataFrame | None = None,
):
    st.session_state["df_pedidos"] = pd.DataFrame(
        resultados
    )

    st.session_state["falhas"] = falhas
    st.session_state["nao_encontrados"] = nao_encontrados

    st.session_state["df_tarefas"] = (
        pd.DataFrame(tarefas_flat)
        if tarefas_flat
        else pd.DataFrame(columns=COLUNAS_TAREFAS)
    )

    st.session_state["tarefas_falhas"] = tarefas_falhas
    st.session_state["origem_consulta"] = origem_consulta

    if df_sql_selecionado is not None:
        st.session_state[
            "df_sql_selecionado"
        ] = df_sql_selecionado.copy()


def executar_api_para_lista(
    lista_pedidos: list[str],
    paralelismo: int,
    origem_consulta: str,
    df_sql_selecionado: pd.DataFrame | None = None,
):
    if not lista_pedidos:
        st.warning(
            "Nenhum pedido foi selecionado para consulta."
        )
        return

    if len(lista_pedidos) > MAX_PEDIDOS_POR_CONSULTA:
        st.warning(
            f"A seleção possui {len(lista_pedidos):,} pedidos. "
            f"O limite configurado é "
            f"{MAX_PEDIDOS_POR_CONSULTA:,}."
            .replace(",", ".")
        )
        return

    recursos = get_recursos()

    with st.spinner("Autenticando na API do Infor..."):
        token = get_token()

    resultados, nao_encontrados, falhas = (
        consultar_pedidos(
            lista_pedidos,
            token,
            recursos,
            max_workers=paralelismo,
        )
    )

    if not resultados:
        limpar_resultados()

        mensagem = (
            "Nenhum pedido foi encontrado no WMS "
            "para a seleção informada."
        )

        if falhas:
            mensagem += (
                f" ({len(falhas)} pedido(s) "
                "tiveram erro de consulta.)"
            )

        st.warning(mensagem)
        return

    pedidos_elegiveis = sorted(
        {
            str(r["Pedido"]).strip()
            for r in resultados
            if pedido_elegivel_tarefas(
                r["StatusCod"]
            )
        }
    )

    tarefas_flat, tarefas_falhas = (
        consultar_tarefas(
            pedidos_elegiveis,
            token,
            recursos,
            max_workers=paralelismo,
        )
    )

    salvar_resultados(
        resultados=resultados,
        nao_encontrados=nao_encontrados,
        falhas=falhas,
        tarefas_flat=tarefas_flat,
        tarefas_falhas=tarefas_falhas,
        origem_consulta=origem_consulta,
        df_sql_selecionado=df_sql_selecionado,
    )


# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.header("Filtros de consulta")

    origem = st.radio(
        "Origem dos pedidos",
        [
            "SQL Server",
            "Consulta manual",
        ],
    )

    paralelismo = st.slider(
        "Consultas simultâneas na API",
        min_value=1,
        max_value=MAX_CONSULTAS_POR_USUARIO,
        value=8,
        help=(
            "Quantidade de requisições paralelas enviadas "
            "à API do Infor."
        ),
    )

    st.caption(
        f"Limite configurado: "
        f"{MAX_PEDIDOS_POR_CONSULTA:,} pedidos por consulta."
        .replace(",", ".")
    )


# =========================================================
# MODO SQL SERVER
# =========================================================
if origem == "SQL Server":
    hoje = date.today()
    default_ini = max(
        DATA_MINIMA_SQL,
        hoje - timedelta(days=7),
    )

    with st.sidebar.form("form_sql"):
        st.subheader("Base SQL")

        data_inicial = st.date_input(
            "Data inicial",
            value=default_ini,
            min_value=DATA_MINIMA_SQL,
            max_value=hoje,
            format="DD/MM/YYYY",
        )

        data_final = st.date_input(
            "Data final",
            value=hoje,
            min_value=DATA_MINIMA_SQL,
            max_value=hoje,
            format="DD/MM/YYYY",
        )

        carregar_sql = st.form_submit_button(
            "🔄 Carregar filtros",
            use_container_width=True,
        )

    if carregar_sql:
        if data_final < data_inicial:
            st.sidebar.warning(
                "A data final deve ser maior ou igual "
                "à data inicial."
            )
        else:
            try:
                with st.spinner(
                    "Consultando a base no SQL Server..."
                ):
                    df_base_sql = carregar_base_sql(
                        data_inicial,
                        data_final,
                    )

                st.session_state["df_base_sql"] = (
                    df_base_sql
                )

                st.session_state[
                    "periodo_sql"
                ] = (
                    data_inicial,
                    data_final,
                )

            except Exception as e:
                st.error(
                    "Erro ao consultar o SQL Server: "
                    f"{e}"
                )

    df_base_sql = st.session_state.get(
        "df_base_sql",
        pd.DataFrame(columns=COLUNAS_SQL),
    )

    if not df_base_sql.empty:
        periodo_sql = st.session_state.get(
            "periodo_sql"
        )

        if periodo_sql:
            st.info(
                "Base carregada do SQL Server: "
                f"**{periodo_sql[0].strftime('%d/%m/%Y')}** "
                "até "
                f"**{periodo_sql[1].strftime('%d/%m/%Y')}** "
                f"• **{df_base_sql['PEDIDO'].nunique():,} "
                "pedidos**"
                .replace(",", ".")
            )

        filiais_disponiveis = sorted(
            df_base_sql["FILIAL"]
            .dropna()
            .unique()
            .tolist()
        )

        tipos_disponiveis = sorted(
            df_base_sql["TIPO_ATENDIMENTO"]
            .dropna()
            .unique()
            .tolist()
        )

        codigos_disponiveis = sorted(
            df_base_sql["CODIGO"]
            .dropna()
            .unique()
            .tolist()
        )

        with st.sidebar.form("form_filtros_sql"):
            st.subheader("Filtros operacionais")

            filiais_selecionadas = st.multiselect(
                "Filial",
                options=filiais_disponiveis,
                default=filiais_disponiveis,
            )

            tipos_selecionados = st.multiselect(
                "Tipo de atendimento",
                options=tipos_disponiveis,
                default=tipos_disponiveis,
            )

            codigos_selecionados = st.multiselect(
                "Código",
                options=codigos_disponiveis,
                default=codigos_disponiveis,
                help=(
                    "Código validado no padrão 000000-000. "
                    "Casos fora do padrão aparecem como "
                    "'Sem código'."
                ),
            )

            consultar_sql_api = st.form_submit_button(
                "🔍 Consultar no Infor",
                use_container_width=True,
            )

        df_preview_sql = aplicar_filtros_sql(
            df_base_sql,
            filiais_selecionadas,
            tipos_selecionados,
            codigos_selecionados,
        )

        st.subheader("🧾 Seleção do SQL Server")

        p1, p2, p3, p4 = st.columns(4)

        p1.metric(
            "Pedidos selecionados",
            f"{df_preview_sql['PEDIDO'].nunique():,}"
            .replace(",", "."),
        )

        p2.metric(
            "Registros SQL",
            f"{len(df_preview_sql):,}"
            .replace(",", "."),
        )

        p3.metric(
            "Filiais",
            df_preview_sql["FILIAL"].nunique(),
        )

        p4.metric(
            "Tipos de atendimento",
            df_preview_sql[
                "TIPO_ATENDIMENTO"
            ].nunique(),
        )

        with st.expander(
            "Ver pedidos selecionados no SQL",
            expanded=False,
        ):
            preview = df_preview_sql.copy()

            preview["DATA_CRIACAO"] = (
                preview["DATA_CRIACAO"]
                .dt.strftime("%d/%m/%Y")
            )

            st.dataframe(
                preview,
                use_container_width=True,
                hide_index=True,
            )

        if consultar_sql_api:
            lista_pedidos = (
                df_preview_sql["PEDIDO"]
                .dropna()
                .astype(str)
                .str.strip()
                .loc[lambda s: s != ""]
                .drop_duplicates()
                .tolist()
            )

            try:
                executar_api_para_lista(
                    lista_pedidos=lista_pedidos,
                    paralelismo=paralelismo,
                    origem_consulta="SQL Server",
                    df_sql_selecionado=df_preview_sql,
                )
            except requests.exceptions.HTTPError as e:
                st.error(
                    f"Erro de autenticação/API: {e}"
                )
            except Exception as e:
                st.error(
                    f"Erro inesperado: {e}"
                )

    else:
        st.info(
            "Selecione o período na barra lateral e clique "
            "em **Carregar filtros** para consultar o "
            "SQL Server."
        )


# =========================================================
# MODO MANUAL
# =========================================================
else:
    with st.sidebar.form("form_manual"):
        modo_busca = st.radio(
            "Tipo de busca manual",
            [
                "Faixa de pedidos",
                "Lista de pedidos (OR)",
            ],
        )

        if modo_busca == "Faixa de pedidos":
            inicio = st.number_input(
                "Pedido inicial",
                min_value=1,
                step=1,
            )

            fim = st.number_input(
                "Pedido final",
                min_value=1,
                step=1,
            )

        else:
            texto_pedidos = st.text_area(
                "Pedidos",
                placeholder=(
                    "pedido1 or pedido2 or pedido3"
                ),
                help=(
                    "Separe os pedidos com 'or'."
                ),
            )

        consultar_manual = st.form_submit_button(
            "🔍 Consultar",
            use_container_width=True,
        )

    if consultar_manual:
        try:
            if modo_busca == "Faixa de pedidos":
                if int(fim) < int(inicio):
                    st.warning(
                        "O pedido final deve ser maior "
                        "ou igual ao pedido inicial."
                    )
                    st.stop()

                lista_pedidos = [
                    str(x)
                    for x in range(
                        int(inicio),
                        int(fim) + 1,
                    )
                ]

            else:
                lista_pedidos = parse_lista_or(
                    texto_pedidos
                )

                if not lista_pedidos:
                    st.warning(
                        "Informe ao menos um pedido."
                    )
                    st.stop()

            executar_api_para_lista(
                lista_pedidos=lista_pedidos,
                paralelismo=paralelismo,
                origem_consulta="Consulta manual",
            )

        except requests.exceptions.HTTPError as e:
            st.error(
                f"Erro de autenticação/API: {e}"
            )

        except Exception as e:
            st.error(
                f"Erro inesperado: {e}"
            )


# =========================================================
# RESULTADOS
# =========================================================
if "df_pedidos" in st.session_state:
    df_api = st.session_state[
        "df_pedidos"
    ].copy()

    df_api["Pedido"] = (
        df_api["Pedido"]
        .astype(str)
        .str.strip()
    )

    df_api["Peças"] = pd.to_numeric(
        df_api["Peças"],
        errors="coerce",
    ).fillna(0)

    origem_resultado = st.session_state.get(
        "origem_consulta",
        "-",
    )

    # -----------------------------------------------------
    # CRUZAMENTO SQL + API
    # -----------------------------------------------------
    df_sql_selecionado = st.session_state.get(
        "df_sql_selecionado"
    )

    if (
        origem_resultado == "SQL Server"
        and isinstance(
            df_sql_selecionado,
            pd.DataFrame,
        )
        and not df_sql_selecionado.empty
    ):
        df_sql_join = (
            df_sql_selecionado
            .copy()
            .drop_duplicates(
                subset=["PEDIDO"],
                keep="first",
            )
        )

        df_sql_join["PEDIDO"] = (
            df_sql_join["PEDIDO"]
            .astype(str)
            .str.strip()
        )

        df = df_api.merge(
            df_sql_join,
            left_on="Pedido",
            right_on="PEDIDO",
            how="left",
        )

        df = df.drop(
            columns=["PEDIDO"],
            errors="ignore",
        )

    else:
        df = df_api.copy()

        df["FILIAL"] = ""
        df["DATA_CRIACAO"] = pd.NaT
        df["MENSAGEM_NOTA"] = ""
        df["CODIGO"] = ""
        df["TIPO_ATENDIMENTO"] = ""

    falhas = st.session_state.get(
        "falhas",
        [],
    )

    nao_encontrados = st.session_state.get(
        "nao_encontrados",
        [],
    )

    if falhas:
        st.warning(
            f"⚠️ {len(falhas)} pedido(s) "
            "não puderam ser consultados "
            f"(erro de comunicação/API): {falhas}"
        )

    if nao_encontrados:
        with st.expander(
            f"ℹ️ {len(nao_encontrados)} "
            "pedido(s) do SQL/lista sem correspondente no WMS"
        ):
            st.write(
                sorted(
                    nao_encontrados,
                    key=str,
                )
            )

    st.caption(
        f"Origem da seleção: **{origem_resultado}**"
    )

    # -----------------------------------------------------
    # FILTROS DO DASHBOARD
    # -----------------------------------------------------
    st.divider()
    st.subheader("🎛️ Filtros do dashboard")

    fd1, fd2, fd3, fd4 = st.columns(4)

    with fd1:
        status_disponiveis = sorted(
            df["Status"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        status_selecionados = st.multiselect(
            "Status WMS",
            options=status_disponiveis,
            default=status_disponiveis,
        )

    with fd2:
        mensagens_disponiveis = sorted(
            df["MensagemNotaAPI"]
            .fillna("")
            .astype(str)
            .unique()
            .tolist()
        )

        mensagens_selecionadas = st.multiselect(
            "Mensagem/Nota API",
            options=mensagens_disponiveis,
            default=mensagens_disponiveis,
        )

    with fd3:
        if "TIPO_ATENDIMENTO" in df.columns:
            tipos_dashboard = sorted(
                df["TIPO_ATENDIMENTO"]
                .dropna()
                .astype(str)
                .loc[
                    lambda s: s.str.strip() != ""
                ]
                .unique()
                .tolist()
            )
        else:
            tipos_dashboard = []

        tipos_dashboard_sel = st.multiselect(
            "Tipo atendimento",
            options=tipos_dashboard,
            default=tipos_dashboard,
            disabled=not bool(tipos_dashboard),
        )

    with fd4:
        if "CODIGO" in df.columns:
            codigos_dashboard = sorted(
                df["CODIGO"]
                .dropna()
                .astype(str)
                .loc[
                    lambda s: s.str.strip() != ""
                ]
                .unique()
                .tolist()
            )
        else:
            codigos_dashboard = []

        codigos_dashboard_sel = st.multiselect(
            "Código",
            options=codigos_dashboard,
            default=codigos_dashboard,
            disabled=not bool(codigos_dashboard),
        )

    df_filtrado = df.copy()

    if status_selecionados:
        df_filtrado = df_filtrado[
            df_filtrado["Status"].isin(
                status_selecionados
            )
        ]
    else:
        df_filtrado = df_filtrado.iloc[0:0]

    if mensagens_selecionadas:
        df_filtrado = df_filtrado[
            df_filtrado["MensagemNotaAPI"].isin(
                mensagens_selecionadas
            )
        ]
    else:
        df_filtrado = df_filtrado.iloc[0:0]

    if tipos_dashboard:
        if tipos_dashboard_sel:
            df_filtrado = df_filtrado[
                df_filtrado[
                    "TIPO_ATENDIMENTO"
                ].isin(tipos_dashboard_sel)
            ]
        else:
            df_filtrado = df_filtrado.iloc[0:0]

    if codigos_dashboard:
        if codigos_dashboard_sel:
            df_filtrado = df_filtrado[
                df_filtrado[
                    "CODIGO"
                ].isin(codigos_dashboard_sel)
            ]
        else:
            df_filtrado = df_filtrado.iloc[0:0]

    # -----------------------------------------------------
    # TAREFAS DOS PEDIDOS FILTRADOS
    # -----------------------------------------------------
    pedidos_filtrados = set(
        df_filtrado["Pedido"]
        .astype(str)
        .str.strip()
    )

    df_tarefas_todas = st.session_state.get(
        "df_tarefas",
        pd.DataFrame(
            columns=COLUNAS_TAREFAS
        ),
    )

    df_tarefas = (
        df_tarefas_todas[
            df_tarefas_todas["Pedido"]
            .astype(str)
            .str.strip()
            .isin(pedidos_filtrados)
        ].copy()
    )

    tarefas_falhas = [
        p
        for p in st.session_state.get(
            "tarefas_falhas",
            [],
        )
        if str(p).strip() in pedidos_filtrados
    ]

    # -----------------------------------------------------
    # FILTRO DE COLABORADORES
    # -----------------------------------------------------
    if not df_tarefas.empty:
        usuarios_disponiveis = sorted(
            df_tarefas["UserKey"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda x: x != ""]
            .unique()
            .tolist()
        )

        usuarios_selecionados = st.multiselect(
            "👤 Filtrar colaboradores "
            "(tarefas, rankings e hora a hora)",
            options=usuarios_disponiveis,
            default=usuarios_disponiveis,
        )

        if usuarios_selecionados:
            df_tarefas_filtrado = (
                df_tarefas[
                    df_tarefas["UserKey"]
                    .astype(str)
                    .str.strip()
                    .isin(
                        usuarios_selecionados
                    )
                ].copy()
            )
        else:
            df_tarefas_filtrado = (
                pd.DataFrame(
                    columns=df_tarefas.columns
                )
            )

    else:
        usuarios_disponiveis = []
        usuarios_selecionados = []
        df_tarefas_filtrado = pd.DataFrame(
            columns=COLUNAS_TAREFAS
        )

    # -----------------------------------------------------
    # INDICADORES
    # -----------------------------------------------------
    st.divider()

    total_pedidos = (
        df_filtrado["Pedido"].nunique()
    )

    total_pecas = int(
        df_filtrado["Peças"].sum()
    )

    media_pecas = (
        round(
            df_filtrado["Peças"].mean(),
            1,
        )
        if total_pedidos
        else 0
    )

    status_predominante = (
        df_filtrado["Status"].mode()[0]
        if not df_filtrado.empty
        else "-"
    )

    col1, col2, col3, col4, col5 = (
        st.columns(5)
    )

    col1.metric(
        "📄 Pedidos",
        total_pedidos,
    )

    col2.metric(
        "📦 Total de Peças",
        f"{total_pecas:,}".replace(
            ",",
            ".",
        ),
    )

    col3.metric(
        "📊 Média de Peças/Pedido",
        media_pecas,
    )

    col4.metric(
        "🏷️ Status Predominante",
        status_predominante,
    )

    col5.metric(
        "🗂️ Tarefas",
        len(df_tarefas_filtrado),
    )

    # -----------------------------------------------------
    # GRÁFICOS DOS PEDIDOS
    # -----------------------------------------------------
    st.divider()

    g1, g2 = st.columns(2)

    with g1:
        st.subheader("Peças por Status")

        df_status = (
            df_filtrado
            .groupby(
                "Status",
                as_index=False,
            )["Peças"]
            .sum()
            .sort_values(
                "Peças",
                ascending=False,
            )
        )

        if not df_status.empty:
            fig_bar = px.bar(
                df_status,
                x="Status",
                y="Peças",
                text_auto=True,
            )

            fig_bar.update_traces(
                marker_color="#1f77b4"
            )

            fig_bar.update_layout(
                showlegend=False
            )

            st.plotly_chart(
                fig_bar,
                use_container_width=True,
            )

    with g2:
        st.subheader(
            "Distribuição de Pedidos por Status"
        )

        if not df_filtrado.empty:
            fig_pie = px.pie(
                df_filtrado,
                names="Status",
                hole=0.45,
            )

            st.plotly_chart(
                fig_pie,
                use_container_width=True,
            )

    # -----------------------------------------------------
    # TAREFAS POR STATUS
    # -----------------------------------------------------
    if not df_tarefas_filtrado.empty:
        st.subheader("Tarefas por Status")

        contagem_tarefas = (
            df_tarefas_filtrado["Status"]
            .value_counts()
            .reset_index()
        )

        contagem_tarefas.columns = [
            "Status",
            "Qtd Tarefas",
        ]

        fig_tarefas = px.bar(
            contagem_tarefas,
            x="Status",
            y="Qtd Tarefas",
            color="Status",
            text_auto=True,
        )

        fig_tarefas.update_layout(
            showlegend=False
        )

        st.plotly_chart(
            fig_tarefas,
            use_container_width=True,
        )

    # -----------------------------------------------------
    # HORA A HORA
    # -----------------------------------------------------
    hora_a_hora = calcular_tarefas_hora_a_hora(
        df_tarefas_filtrado
    )

    if not hora_a_hora.empty:
        st.divider()

        st.subheader(
            "⏱️ Acompanhamento Hora a Hora "
            "(Tarefas Concluídas)"
        )

        st.caption(
            f"Baseado no EndTime, convertido para "
            f"{FUSO_HORARIO}. Considera somente tarefas "
            f"com status "
            f"'{traduzir_status_tarefa(TASK_STATUS_CONCLUIDO)}'."
        )

        datas_disponiveis = sorted(
            hora_a_hora["Data"]
            .dropna()
            .unique(),
            reverse=True,
        )

        data_selecionada = st.selectbox(
            "📅 Data do acompanhamento",
            options=datas_disponiveis,
            index=0,
            format_func=lambda x: x.strftime(
                "%d/%m/%Y"
            ),
        )

        hora_a_hora_dia = (
            hora_a_hora[
                hora_a_hora["Data"]
                == data_selecionada
            ]
            .copy()
            .sort_values("HoraOrdenacao")
        )

        horas_base = pd.DataFrame(
            {
                "Hora": [
                    f"{h:02d}h"
                    for h in range(24)
                ],
                "HoraNum": list(range(24)),
            }
        )

        hora_a_hora_dia["HoraNum"] = (
            hora_a_hora_dia[
                "HoraOrdenacao"
            ].dt.hour
        )

        hora_a_hora_dia = (
            horas_base
            .merge(
                hora_a_hora_dia[
                    [
                        "HoraNum",
                        "Tarefas Concluídas",
                        "Peças",
                    ]
                ],
                on="HoraNum",
                how="left",
            )
            .fillna(
                {
                    "Tarefas Concluídas": 0,
                    "Peças": 0,
                }
            )
            .sort_values("HoraNum")
        )

        hora_a_hora_dia[
            "Tarefas Concluídas"
        ] = (
            hora_a_hora_dia[
                "Tarefas Concluídas"
            ].astype(int)
        )

        hora_a_hora_dia["Peças"] = (
            hora_a_hora_dia["Peças"]
            .round(0)
            .astype(int)
        )

        fig_hh_tarefas = px.bar(
            hora_a_hora_dia,
            x="Hora",
            y="Tarefas Concluídas",
            text="Tarefas Concluídas",
            title="Tarefas concluídas por hora",
        )

        fig_hh_tarefas.update_traces(
            marker_color="#1f77b4",
            textposition="outside",
            textfont=dict(size=16),
            cliponaxis=False,
        )

        fig_hh_tarefas.update_layout(
            showlegend=False,
            xaxis_title=None,
            yaxis_title="Tarefas concluídas",
            height=450,
            margin=dict(
                t=70,
                b=50,
                l=50,
                r=30,
            ),
            xaxis=dict(
                type="category",
                categoryorder="array",
                categoryarray=[
                    f"{h:02d}h"
                    for h in range(24)
                ],
                tickangle=0,
                automargin=True,
            ),
            yaxis=dict(
                rangemode="tozero",
                automargin=True,
            ),
        )

        st.plotly_chart(
            fig_hh_tarefas,
            use_container_width=True,
        )

        fig_hh_pecas = px.bar(
            hora_a_hora_dia,
            x="Hora",
            y="Peças",
            text="Peças",
            title="Peças separadas por hora",
        )

        fig_hh_pecas.update_traces(
            marker_color="#2ca02c",
            textposition="outside",
            textfont=dict(size=16),
            cliponaxis=False,
        )

        fig_hh_pecas.update_layout(
            showlegend=False,
            xaxis_title=None,
            yaxis_title="Peças",
            height=450,
            margin=dict(
                t=70,
                b=50,
                l=50,
                r=30,
            ),
            xaxis=dict(
                type="category",
                categoryorder="array",
                categoryarray=[
                    f"{h:02d}h"
                    for h in range(24)
                ],
                tickangle=0,
                automargin=True,
            ),
            yaxis=dict(
                rangemode="tozero",
                automargin=True,
            ),
        )

        st.plotly_chart(
            fig_hh_pecas,
            use_container_width=True,
        )

        st.dataframe(
            hora_a_hora_dia[
                [
                    "Hora",
                    "Tarefas Concluídas",
                    "Peças",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    # -----------------------------------------------------
    # RANKING DE COLABORADORES
    # -----------------------------------------------------
    ranking = calcular_ranking_colaboradores(
        df_tarefas_filtrado
    )

    if not ranking.empty:
        st.divider()

        st.subheader(
            "🏆 Top 10 Colaboradores - "
            "Tarefas Concluídas"
        )

        st.caption(
            "Tarefas com ReasonCod preenchido não "
            "entram nesse ranking."
        )

        rc1, rc2 = st.columns(2)

        with rc1:
            fig_ranking = px.bar(
                ranking.sort_values(
                    "Tarefas Concluídas"
                ),
                x="Tarefas Concluídas",
                y="Usuário",
                orientation="h",
                text_auto=True,
                title=(
                    "Tarefas concluídas "
                    "por colaborador"
                ),
            )

            fig_ranking.update_layout(
                showlegend=False
            )

            st.plotly_chart(
                fig_ranking,
                use_container_width=True,
            )

        with rc2:
            fig_pecas_hora = px.bar(
                ranking.sort_values(
                    "Peças/Hora"
                ),
                x="Peças/Hora",
                y="Usuário",
                orientation="h",
                text_auto=True,
                title=(
                    "Média de peças "
                    "separadas por hora"
                ),
            )

            fig_pecas_hora.update_layout(
                showlegend=False
            )

            st.plotly_chart(
                fig_pecas_hora,
                use_container_width=True,
            )

        st.dataframe(
            ranking,
            use_container_width=True,
            hide_index=True,
        )

    # -----------------------------------------------------
    # RANKING DE MOTIVOS
    # -----------------------------------------------------
    ranking_motivos = calcular_ranking_motivos(
        df_tarefas_filtrado
    )

    if not ranking_motivos.empty:
        st.divider()

        st.subheader(
            "🚩 Ranking de Motivos (Reason Code)"
        )

        fig_motivos = px.bar(
            ranking_motivos.sort_values(
                "Ocorrências"
            ),
            x="Ocorrências",
            y="Motivo",
            orientation="h",
            text_auto=True,
        )

        fig_motivos.update_layout(
            showlegend=False
        )

        st.plotly_chart(
            fig_motivos,
            use_container_width=True,
        )

        st.dataframe(
            ranking_motivos,
            use_container_width=True,
            hide_index=True,
        )

    # -----------------------------------------------------
    # CLASSIFICAÇÃO POR LOCALIZAÇÃO
    # -----------------------------------------------------
    classificacao_pedidos = (
        classificar_pedidos_por_localizacao(
            df_tarefas_filtrado
        )
    )

    if not classificacao_pedidos.empty:
        st.divider()

        st.subheader(
            "📍 Classificação dos Pedidos "
            "por Localização"
        )

        st.caption(
            "Baseado no fromloc das tarefas: "
            "terminação 000/010 = Baixo, "
            "020 = Médio, demais = Alto."
        )

        cl1, cl2 = st.columns(2)

        with cl1:
            contagem_classificacao = (
                classificacao_pedidos[
                    "Classificação"
                ]
                .value_counts()
                .reset_index()
            )

            contagem_classificacao.columns = [
                "Classificação",
                "Qtd Pedidos",
            ]

            fig_classificacao = px.bar(
                contagem_classificacao,
                x="Classificação",
                y="Qtd Pedidos",
                color="Classificação",
                text_auto=True,
            )

            fig_classificacao.update_layout(
                showlegend=False
            )

            st.plotly_chart(
                fig_classificacao,
                use_container_width=True,
            )

        with cl2:
            st.dataframe(
                classificacao_pedidos.sort_values(
                    "Pedido"
                ),
                use_container_width=True,
                hide_index=True,
                height=380,
            )

    # -----------------------------------------------------
    # TABELA DE PEDIDOS
    # -----------------------------------------------------
    st.divider()
    st.subheader("📋 Pedidos")

    colunas_pedidos = [
        c
        for c in [
            "Pedido",
            "FILIAL",
            "DATA_CRIACAO",
            "CODIGO",
            "TIPO_ATENDIMENTO",
            "StatusCod",
            "Status",
            "Peças",
            "MENSAGEM_NOTA",
            "MensagemNotaAPI",
        ]
        if c in df_filtrado.columns
    ]

    df_exibicao = df_filtrado[
        colunas_pedidos
    ].copy()

    if "DATA_CRIACAO" in df_exibicao.columns:
        df_exibicao["DATA_CRIACAO"] = (
            pd.to_datetime(
                df_exibicao["DATA_CRIACAO"],
                errors="coerce",
            )
            .dt.strftime("%d/%m/%Y")
        )

    st.dataframe(
        df_exibicao,
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "⬇️ Baixar CSV",
        data=df_exibicao
        .to_csv(index=False)
        .encode("utf-8-sig"),
        file_name="pedidos_wms.csv",
        mime="text/csv",
    )

    # -----------------------------------------------------
    # TAREFAS GERADAS
    # -----------------------------------------------------
    if not df_tarefas_filtrado.empty:
        st.divider()

        st.subheader(
            f"🗂️ Tarefas Geradas "
            f"(pedidos com status ≥ "
            f"{STATUS_MIN_TAREFAS})"
        )

        if tarefas_falhas:
            st.warning(
                f"⚠️ Não foi possível consultar "
                f"tarefas de "
                f"{len(tarefas_falhas)} pedido(s): "
                f"{tarefas_falhas}"
            )

        resumo_pedido = (
            df_tarefas_filtrado
            .groupby("Pedido")
            .size()
            .reset_index(
                name="Qtd. de Tarefas"
            )
        )

        st.dataframe(
            resumo_pedido.sort_values(
                "Pedido"
            ),
            use_container_width=True,
            hide_index=True,
        )

        with st.expander(
            "Ver detalhamento das tarefas"
        ):
            st.dataframe(
                df_tarefas_filtrado,
                use_container_width=True,
                hide_index=True,
            )

else:
    if origem == "Consulta manual":
        st.info(
            "Informe os pedidos na barra lateral "
            "e clique em **Consultar**."
        )
