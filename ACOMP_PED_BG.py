```python
"""
Acompanhamento Gráfico - Infor WMS
-----------------------------------
App Streamlit para consultar uma faixa de pedidos no Infor WMS
via API, exibir indicadores e gráficos de status.

Modo mais on time possível, posso usar na bag também.
"""

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import streamlit as st
import plotly.express as px
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# CONFIGURAÇÕES / CREDENCIAIS (via st.secrets)
# =========================================================

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


# =========================================================
# LIMITES
# =========================================================

MAX_PEDIDOS_POR_CONSULTA = 1000
MAX_CONSULTAS_POR_USUARIO = 15
MAX_REQUISICOES_GLOBAIS = 12

CACHE_TTL_SEGUNDOS = 180
CACHE_MAX_ITENS = 3000

HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT = 45
TOKEN_TIMEOUT = 30

# A partir deste status o pedido já possui tarefas geradas
STATUS_MIN_TAREFAS = 29

# Status de tarefa considerado concluído
TASK_STATUS_CONCLUIDO = "9"


# =========================================================
# COLUNAS DAS TAREFAS
# =========================================================

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


def traduzir_status(codigo) -> str:
    """Traduz o código de status do pedido."""
    return STATUS_MAP.get(
        str(codigo).strip(),
        f"Desconhecido ({codigo})"
    )


# =========================================================
# STATUS DAS TAREFAS
# =========================================================

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
    """Traduz o código de status da tarefa."""
    return TASK_STATUS_MAP.get(
        str(codigo).strip(),
        f"Desconhecido ({codigo})"
    )


# =========================================================
# CONFIGURAÇÃO STREAMLIT
# =========================================================

st.set_page_config(
    page_title="Acompanhamento Demanda",
    page_icon="📦",
    layout="wide",
)


# =========================================================
# CACHE TTL
# =========================================================

class CacheTTL:
    """Cache simples em memória com expiração e limite de itens."""

    def __init__(self, ttl: int, max_itens: int):
        self.ttl = ttl
        self.max_itens = max_itens
        self._dados: dict = {}
        self._lock = threading.Lock()

    def get(self, chave):
        """Retorna (achou, valor)."""

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
                valor
            )

            while len(self._dados) > self.max_itens:
                self._dados.pop(next(iter(self._dados)))


# =========================================================
# SESSÃO HTTP
# =========================================================

def criar_sessao() -> requests.Session:
    """Cria sessão HTTP com pool e retry."""

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
            CACHE_MAX_ITENS
        )

        self.cache_tarefas = CacheTTL(
            CACHE_TTL_SEGUNDOS,
            CACHE_MAX_ITENS
        )


@st.cache_resource
def get_recursos() -> Recursos:
    """Conjunto de recursos compartilhado entre os usuários."""
    return Recursos()


# =========================================================
# TOKEN
# =========================================================

@st.cache_data(ttl=3300, show_spinner=False)
def get_token() -> str:
    """
    Obtém e mantém o token OAuth2 em cache.
    TTL de segurança: 55 minutos.
    """

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
    recursos: Recursos
) -> dict | None:

    chave = str(pedido)

    achou, valor = recursos.cache_pedidos.get(chave)

    if achou:
        return valor

    resposta = _requisitar(
        recursos,
        "GET",
        f"{BASE_URL}/{pedido}",
        token
    )

    if resposta.status_code == 404:

        valor = None

    else:

        resposta.raise_for_status()

        dados = resposta.json()

        valor = {
            "orderkey": dados.get("orderkey", pedido),
            "status": dados.get("status", ""),
            "totalqty": dados.get("totalqty", 0),
            "ext_udf_str4": dados.get("ext_udf_str4", ""),
        }

    recursos.cache_pedidos.set(
        chave,
        valor
    )

    return valor


# =========================================================
# CONSULTA DE TAREFAS
# =========================================================

def consulta_tasks(
    pedido,
    token: str,
    recursos: Recursos
) -> list[dict]:

    chave = str(pedido)

    achou, valor = recursos.cache_tarefas.get(chave)

    if achou:
        return valor

    resposta = _requisitar(
        recursos,
        "POST",
        TASKS_URL,
        token,
        json={"orderkey": chave}
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

    recursos.cache_tarefas.set(
        chave,
        valor
    )

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
        flags=re.IGNORECASE
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
    max_workers: int = 8
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
            MAX_REQUISICOES_GLOBAIS
        )
    )

    concluidos = 0

    barra = st.progress(
        0.0,
        text="Consultando pedidos..."
    )

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        futuros = {

            executor.submit(
                consulta_wms,
                pedido,
                token,
                recursos
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
                            "Pedido": dados["orderkey"],
                            "StatusCod": codigo_status,
                            "Status": traduzir_status(
                                codigo_status
                            ),
                            "Peças": dados["totalqty"],
                            "MensagemNota": dados[
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
                    f"Consultando pedidos... "
                    f"({concluidos}/{total})"
                )
            )

    barra.empty()

    return (
        resultados,
        nao_encontrados,
        falhas
    )


# =========================================================
# ELEGIBILIDADE DE TAREFAS
# =========================================================

def pedido_elegivel_tarefas(
    codigo_status
) -> bool:

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
    max_workers: int = 8
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
            MAX_REQUISICOES_GLOBAIS
        )
    )

    concluidos = 0

    barra = st.progress(
        0.0,
        text="Consultando tarefas geradas..."
    )

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        futuros = {

            executor.submit(
                consulta_tasks,
                pedido,
                token,
                recursos
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
                            "Pedido": pedido,
                            "StatusCod": codigo_status,
                            "Status": traduzir_status_tarefa(
                                codigo_status
                            ),
                            "TipoTarefa": tarefa[
                                "tasktype"
                            ],
                            "SKU": tarefa["sku"],
                            "Qtd": tarefa["qty"],
                            "DeLoc": tarefa["fromloc"],
                            "ParaLoc": tarefa["toloc"],
                            "UserKey": tarefa[
                                "userkey"
                            ],
                            "StartTime": tarefa[
                                "starttime"
                            ],
                            "EndTime": tarefa[
                                "endtime"
                            ],
                            "ReasonCod": tarefa[
                                "reasonkey"
                            ],
                        }
                    )

            except requests.exceptions.RequestException:

                falhas.append(pedido)

            concluidos += 1

            barra.progress(
                concluidos / total,
                text=(
                    f"Consultando tarefas geradas... "
                    f"({concluidos}/{total})"
                )
            )

    barra.empty()

    return tarefas_flat, falhas


# =========================================================
# RANKING DE COLABORADORES
# =========================================================

def calcular_ranking_colaboradores(
    df_tarefas: pd.DataFrame,
    top_n: int = 10
) -> pd.DataFrame:

    """
    A partir das tarefas com status Concluído e sem motivo preenchido,
    monta o ranking dos colaboradores.

    Calcula:
    - quantidade de tarefas concluídas
    - tempo médio de separação
    - peças por hora
    """

    colunas_saida = [
        "Usuário",
        "Tarefas Concluídas",
        "Tempo Médio (min)",
        "Peças/Hora"
    ]

    if df_tarefas.empty:

        return pd.DataFrame(
            columns=colunas_saida
        )

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

    # Tarefas com motivo não entram na produtividade
    df = df[
        df["ReasonCod"]
        .astype(str)
        .str.strip()
        == ""
    ]

    if df.empty:

        return pd.DataFrame(
            columns=colunas_saida
        )

    df["StartTime"] = pd.to_datetime(
        df["StartTime"],
        errors="coerce",
        utc=True
    )

    df["EndTime"] = pd.to_datetime(
        df["EndTime"],
        errors="coerce",
        utc=True
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

        return pd.DataFrame(
            columns=colunas_saida
        )

    resumo = (

        df.groupby("UserKey")

        .agg(
            Tarefas_Concluidas=(
                "UserKey",
                "count"
            ),

            Tempo_Medio_Min=(
                "DuracaoMin",
                "mean"
            ),

            Total_Qtd=(
                "Qtd",
                "sum"
            ),

            Total_Min=(
                "DuracaoMin",
                "sum"
            ),
        )

        .reset_index()
    )

    resumo["Total_Horas"] = (
        resumo["Total_Min"] / 60
    )

    resumo["Peças/Hora"] = resumo.apply(
        lambda r:
            round(
                r["Total_Qtd"]
                / r["Total_Horas"],
                1
            )
            if r["Total_Horas"] > 0
            else 0,
        axis=1
    )

    resumo["Tempo Médio (min)"] = (
        resumo["Tempo_Medio_Min"]
        .round(1)
    )

    resumo = resumo.rename(
        columns={
            "UserKey": "Usuário",
            "Tarefas_Concluidas":
                "Tarefas Concluídas"
        }
    )

    resumo = resumo[
        colunas_saida
    ].sort_values(
        "Tarefas Concluídas",
        ascending=False
    ).head(top_n)

    return resumo.reset_index(drop=True)


# =========================================================
# RANKING DE MOTIVOS
# =========================================================

def calcular_ranking_motivos(
    df_tarefas: pd.DataFrame,
    top_n: int = 10
) -> pd.DataFrame:

    colunas_saida = [
        "Motivo",
        "Ocorrências"
    ]

    if df_tarefas.empty:

        return pd.DataFrame(
            columns=colunas_saida
        )

    df = df_tarefas[
        df_tarefas["ReasonCod"]
        .astype(str)
        .str.strip()
        != ""
    ]

    if df.empty:

        return pd.DataFrame(
            columns=colunas_saida
        )

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

def classificar_localizacao(
    fromloc
) -> str:

    loc = str(fromloc).strip()

    if loc.endswith("000") or loc.endswith("010"):
        return "Baixo"

    if loc.endswith("020"):
        return "Médio"

    return "Alto"


def classificar_pedidos_por_localizacao(
    df_tarefas: pd.DataFrame
) -> pd.DataFrame:

    colunas_saida = [
        "Pedido",
        "Classificação"
    ]

    if df_tarefas.empty:

        return pd.DataFrame(
            columns=colunas_saida
        )

    df = df_tarefas.copy()

    df["_Faixa"] = df[
        "DeLoc"
    ].apply(
        classificar_localizacao
    )

    mapa_combinacoes = {

        frozenset({"Alto"}):
            "ALTO",

        frozenset({"Baixo"}):
            "BAIXO",

        frozenset({"Médio"}):
            "MÉDIO",

        frozenset({"Alto", "Baixo"}):
            "Parcial A/B",

        frozenset({"Médio", "Baixo"}):
            "Parcial M/B",

        frozenset({"Alto", "Médio"}):
            "Parcial A/M",

        frozenset(
            {"Alto", "Médio", "Baixo"}
        ):
            "Misto A/M/B",
    }

    resumo = (

        df.groupby("Pedido")["_Faixa"]

        .apply(
            lambda faixas:
                mapa_combinacoes[
                    frozenset(faixas)
                ]
        )

        .reset_index()
    )

    resumo.columns = colunas_saida

    return resumo


# =========================================================
# LIMPAR RESULTADOS
# =========================================================

def limpar_resultados():

    for chave in (
        "df_pedidos",
        "falhas",
        "nao_encontrados",
        "df_tarefas",
        "tarefas_falhas"
    ):

        st.session_state.pop(
            chave,
            None
        )


# =========================================================
# INTERFACE
# =========================================================

st.title(
    "📦 Acompanhamento De Demandas - Infor WMS"
)

st.caption(
    "Consulta de pedidos (shipments) via API REST "
    "do Infor WMS • otimizado para Streamlit Cloud"
)


# =========================================================
# SIDEBAR
# =========================================================

with st.sidebar:

    st.header(
        "Filtros de consulta"
    )

    modo_busca = st.radio(
        "Tipo de busca",
        [
            "Faixa de pedidos",
            "Lista de pedidos (OR)"
        ]
    )

    with st.form("form_consulta"):

        if modo_busca == "Faixa de pedidos":

            inicio = st.number_input(
                "Pedido inicial",
                min_value=1,
                step=1
            )

            fim = st.number_input(
                "Pedido final",
                min_value=1,
                step=1
            )

        else:

            texto_pedidos = st.text_area(
                "Pedidos",
                placeholder=(
                    "pedido1 or pedido2 or pedido3"
                ),
                help=(
                    "Separe os pedidos com 'or' "
                    "(não diferencia maiúsculas/"
                    "minúsculas)."
                ),
            )

        paralelismo = st.slider(
            "Consultas simultâneas",
            min_value=1,
            max_value=MAX_CONSULTAS_POR_USUARIO,
            value=8,
            help=(
                "Número de requisições feitas "
                "em paralelo."
            ),
        )

        consultar = st.form_submit_button(
            "🔍 Consultar",
            use_container_width=True
        )

    st.caption(
        f"Limite de "
        f"{MAX_PEDIDOS_POR_CONSULTA} "
        f"pedidos por consulta."
    )


# =========================================================
# EXECUÇÃO DA CONSULTA
# =========================================================

if consultar:

    try:

        if modo_busca == "Faixa de pedidos":

            if int(fim) < int(inicio):

                st.warning(
                    "O pedido final deve ser maior "
                    "ou igual ao pedido inicial."
                )

                st.stop()

            lista_pedidos = list(
                range(
                    int(inicio),
                    int(fim) + 1
                )
            )

        else:

            lista_pedidos = parse_lista_or(
                texto_pedidos
            )

            if not lista_pedidos:

                st.warning(
                    "Informe ao menos um pedido, "
                    "separado por 'or'."
                )

                st.stop()

        if len(lista_pedidos) > MAX_PEDIDOS_POR_CONSULTA:

            st.warning(
                f"A consulta tem "
                f"{len(lista_pedidos)} pedidos. "
                f"O limite é de "
                f"{MAX_PEDIDOS_POR_CONSULTA}."
            )

            st.stop()

        recursos = get_recursos()

        with st.spinner(
            "Autenticando..."
        ):

            token = get_token()

        resultados, nao_encontrados, falhas = (
            consultar_pedidos(
                lista_pedidos,
                token,
                recursos,
                max_workers=paralelismo
            )
        )

        if not resultados:

            limpar_resultados()

            mensagem = (
                "Nenhum pedido foi encontrado "
                "para a consulta informada."
            )

            if falhas:

                mensagem += (
                    f" ({len(falhas)} pedido(s) "
                    f"tiveram erro de consulta.)"
                )

            st.warning(mensagem)

        else:

            st.session_state[
                "df_pedidos"
            ] = pd.DataFrame(resultados)

            st.session_state[
                "falhas"
            ] = falhas

            st.session_state[
                "nao_encontrados"
            ] = nao_encontrados

            # Pedidos com status >= 29
            # já geraram tarefas
            pedidos_elegiveis = sorted(
                {
                    str(r["Pedido"])

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
                    max_workers=paralelismo
                )
            )

            st.session_state[
                "df_tarefas"
            ] = (

                pd.DataFrame(
                    tarefas_flat
                )

                if tarefas_flat

                else pd.DataFrame(
                    columns=COLUNAS_TAREFAS
                )
            )

            st.session_state[
                "tarefas_falhas"
            ] = tarefas_falhas

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

    df = st.session_state[
        "df_pedidos"
    ]

    falhas = st.session_state.get(
        "falhas",
        []
    )

    nao_encontrados = st.session_state.get(
        "nao_encontrados",
        []
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
            "número(s) sem pedido correspondente no WMS"
        ):

            st.write(
                sorted(
                    nao_encontrados,
                    key=str
                )
            )


    # =====================================================
    # FILTRO POR MENSAGEM / NOTA
    # =====================================================

    mensagens = [
        "Todos"
    ] + sorted(
        df["MensagemNota"]
        .dropna()
        .unique()
        .tolist()
    )

    filtro = st.selectbox(
        "Filtrar por Mensagem/Nota",
        mensagens
    )

    df_filtrado = (

        df

        if filtro == "Todos"

        else df[
            df["MensagemNota"]
            == filtro
        ]
    )


    # =====================================================
    # FILTRO DAS TAREFAS PELOS PEDIDOS
    # =====================================================

    pedidos_filtrados = set(
        df_filtrado[
            "Pedido"
        ].astype(str)
    )

    df_tarefas_todas = (
        st.session_state.get(
            "df_tarefas",
            pd.DataFrame(
                columns=COLUNAS_TAREFAS
            )
        )
    )

    df_tarefas = (
        df_tarefas_todas[
            df_tarefas_todas[
                "Pedido"
            ]
            .astype(str)
            .isin(pedidos_filtrados)
        ]
    )

    tarefas_falhas = [

        p

        for p in st.session_state.get(
            "tarefas_falhas",
            []
        )

        if str(p) in pedidos_filtrados
    ]


    # =====================================================
    # INDICADORES
    # =====================================================

    st.divider()

    total_pedidos = (
        df_filtrado[
            "Pedido"
        ].nunique()
    )

    total_pecas = int(
        df_filtrado[
            "Peças"
        ].sum()
    )

    media_pecas = (

        round(
            df_filtrado[
                "Peças"
            ].mean(),
            1
        )

        if total_pedidos

        else 0
    )

    status_predominante = (

        df_filtrado[
            "Status"
        ].mode()[0]

        if not df_filtrado.empty

        else "-"
    )


    col1, col2, col3, col4, col5 = (
        st.columns(5)
    )

    col1.metric(
        "📄 Pedidos",
        total_pedidos
    )

    col2.metric(
        "📦 Total de Peças",
        f"{total_pecas:,}".replace(
            ",",
            "."
        )
    )

    col3.metric(
        "📊 Média de Peças/Pedido",
        media_pecas
    )

    col4.metric(
        "🏷️ Status Predominante",
        status_predominante
    )

    col5.metric(
        "🗂️ Tarefas Geradas",
        len(df_tarefas)
    )


    # =====================================================
    # GRÁFICOS
    # =====================================================

    st.divider()

    g1, g2 = st.columns(2)


    # -----------------------------------------------------
    # PEÇAS POR STATUS
    # -----------------------------------------------------

    with g1:

        st.subheader(
            "Peças por Status"
        )

        df_status = (

            df_filtrado

            .groupby(
                "Status",
                as_index=False
            )["Peças"]

            .sum()

            .sort_values(
                "Peças",
                ascending=False
            )
        )

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
            use_container_width=True
        )


    # -----------------------------------------------------
    # DISTRIBUIÇÃO DE PEDIDOS
    # -----------------------------------------------------

    with g2:

        st.subheader(
            "Distribuição de Pedidos por Status"
        )

        fig_pie = px.pie(
            df_filtrado,
            names="Status",
            hole=0.45,
        )

        st.plotly_chart(
            fig_pie,
            use_container_width=True
        )


    # =====================================================
    # TAREFAS POR STATUS
    # =====================================================

    if not df_tarefas.empty:

        st.subheader(
            "Tarefas por Status"
        )

        contagem_tarefas = (
            df_tarefas[
                "Status"
            ]
            .value_counts()
            .reset_index()
        )

        contagem_tarefas.columns = [
            "Status",
            "Qtd Tarefas"
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
            use_container_width=True
        )


    # =====================================================
    # FILTRO DE COLABORADORES
    # =====================================================

    if not df_tarefas.empty:

        usuarios_disponiveis = sorted(

            df_tarefas[
                "UserKey"
            ]

            .dropna()

            .astype(str)

            .str.strip()

            .loc[
                lambda x: x != ""
            ]

            .unique()

            .tolist()
        )

        usuarios_selecionados = (
            st.multiselect(

                "👤 Filtrar colaboradores "
                "dos rankings",

                options=usuarios_disponiveis,

                default=usuarios_disponiveis,

                help=(
                    "Esse filtro afeta somente "
                    "o ranking de tarefas concluídas, "
                    "tempo médio e Peças/Hora."
                ),
            )
        )

        if usuarios_selecionados:

            df_tarefas_ranking = (
                df_tarefas[
                    df_tarefas[
                        "UserKey"
                    ]
                    .astype(str)
                    .isin(
                        usuarios_selecionados
                    )
                ].copy()
            )

        else:

            df_tarefas_ranking = (
                pd.DataFrame(
                    columns=df_tarefas.columns
                )
            )

    else:

        usuarios_disponiveis = []

        usuarios_selecionados = []

        df_tarefas_ranking = (
            pd.DataFrame(
                columns=df_tarefas.columns
            )
        )


    # =====================================================
    # RANKING DE COLABORADORES
    # =====================================================

    ranking = (
        calcular_ranking_colaboradores(
            df_tarefas_ranking
        )
    )

    if not ranking.empty:

        st.divider()

        st.subheader(
            "🏆 Top 10 Colaboradores - "
            "Tarefas Concluídas"
        )

        st.caption(
            "Tarefas com motivo (ReasonCod) "
            "preenchido não entram nesse ranking."
        )

        rc1, rc2 = st.columns(2)


        # -------------------------------------------------
        # TAREFAS CONCLUÍDAS
        # -------------------------------------------------

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
                use_container_width=True
            )


        # -------------------------------------------------
        # PEÇAS POR HORA
        # -------------------------------------------------

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
                use_container_width=True
            )


        # -------------------------------------------------
        # TABELA DO RANKING
        # -------------------------------------------------

        st.dataframe(
            ranking,
            use_container_width=True,
            hide_index=True
        )

        st.caption(

            "Tempo médio calculado a partir "
            "de StartTime/EndTime das tarefas "
            "com status "

            f"'{traduzir_status_tarefa(
                TASK_STATUS_CONCLUIDO
            )}'. "

            "Peças/Hora = soma de peças separadas "
            "÷ soma de horas trabalhadas pelo "
            "colaborador."
        )


    # =====================================================
    # RANKING DE MOTIVOS
    # =====================================================

    ranking_motivos = (
        calcular_ranking_motivos(
            df_tarefas
        )
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
            use_container_width=True
        )

        st.dataframe(
            ranking_motivos,
            use_container_width=True,
            hide_index=True
        )


    # =====================================================
    # CLASSIFICAÇÃO POR LOCALIZAÇÃO
    # =====================================================

    classificacao_pedidos = (
        classificar_pedidos_por_localizacao(
            df_tarefas
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
            "020 = Médio, demais = Alto. "
            "Pedidos com mais de uma faixa "
            "aparecem como parcial/misto."
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
                "Qtd Pedidos"
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
                use_container_width=True
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


    # =====================================================
    # TABELA DE PEDIDOS
    # =====================================================

    st.divider()

    st.subheader(
        "📋 Pedidos"
    )

    st.dataframe(
        df_filtrado,
        use_container_width=True,
        hide_index=True
    )


    # =====================================================
    # DOWNLOAD CSV
    # =====================================================

    st.download_button(

        "⬇️ Baixar CSV",

        data=df_filtrado
        .to_csv(index=False)
        .encode("utf-8"),

        file_name="pedidos_wms.csv",

        mime="text/csv",
    )


    # =====================================================
    # TAREFAS GERADAS
    # =====================================================

    if not df_tarefas.empty:

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

            df_tarefas

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

            hide_index=True
        )

        with st.expander(
            "Ver detalhamento das tarefas"
        ):

            st.dataframe(

                df_tarefas,

                use_container_width=True,

                hide_index=True
            )

else:

    st.info(
        "Informe a faixa de pedidos na barra "
        "lateral e clique em **Consultar**."
    )
