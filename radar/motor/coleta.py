"""Coleta e enriquecimento de imprensa.

Fonte primária: Google News RSS. É interface não oficial — pode mudar sem aviso,
e é o risco número um do produto. Por isso o coletor de RSS nativo dos portais
roda em paralelo desde o primeiro dia, e não como plano B teórico.

A armadilha central: desde 2024 o link do item vem ofuscado
(/rss/articles/CBMi...) e NÃO redireciona mais — a URL real é resolvida por
uma chamada a batchexecute com uma assinatura que só existe na página do
artigo. Guardar o link ofuscado faria a mesma matéria entrar de novo a cada
coleta, porque o identificador muda.
"""
from __future__ import annotations

import gzip
import html as _html
import json
import re
import threading
import time
import unicodedata
import zlib
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
      "(KHTML, like Gecko) Version/17.0 Safari/605.1.15")
# identificação honesta para o fetch do artigo: o IP da hospedagem é compartilhado
# e vira bloqueio se a gente parecer raspador anônimo
UA_ARTIGO = "RadarPLAN/1.0 (monitoramento de imprensa; +https://plancoded.com.br)"

GNEWS_RSS = "https://news.google.com/rss/search"
GNEWS_BATCH = "https://news.google.com/_/DotsSplashUi/data/batchexecute?rpcids=Fbv4je"

LIXO_QUERY = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    "fbclid", "gclid", "gbraid", "wbraid", "msclkid", "igshid", "mc_cid", "mc_eid",
    "ref", "referrer", "amp", "outputType", "_ga", "s_cid", "xtor",
}


# --------------------------------------------------------------------- rede
MAX_CORPO = 6 * 1024 * 1024      # matéria maior que isso é página de listagem, não notícia

# Espaçamento por domínio. Sem isto, 12 threads batem no mesmo portal ao mesmo
# tempo e ele derruba a conexão sem devolver código — foi o que produziu 50
# "http 0" na primeira rodada, concentrados justamente nos portais locais que
# mais importam. O IP da hospedagem é compartilhado: apanhar aqui custa caro.
ESPACO_POR_DOMINIO = 1.2
# A infraestrutura do Google não precisa deste cuidado, e tratá-la como portal
# pequeno serializa as ~200 buscas de assinatura numa fila só — o gargalo passa
# a ser a nossa própria educação.
ESPACO_ESPECIAL = {"news.google.com": 0.12}
_ultimo: dict[str, float] = {}
_tranca = threading.Lock()


def _aguardar_vez(dominio: str) -> None:
    espaco = ESPACO_ESPECIAL.get(dominio, ESPACO_POR_DOMINIO)
    while True:
        with _tranca:
            agora = time.monotonic()
            quando = _ultimo.get(dominio, 0.0)
            if agora - quando >= espaco:
                _ultimo[dominio] = agora
                return
            falta = espaco - (agora - quando)
        time.sleep(falta)


def buscar(url: str, *, ua: str = UA, timeout: int = 30, dados: bytes | None = None,
           cabecalhos: dict | None = None, prazo_total: int = 40,
           espacar: bool = True) -> tuple[int, bytes]:
    """Requisição com prazo TOTAL, não só de socket.

    O `timeout` do urlopen cobre cada operação de socket isolada. Um servidor
    que responde devagar mas sem parar nunca dispara esse timeout e trava a
    thread para sempre — foi o que travou a primeira rodada de enriquecimento
    em 168 matérias. A leitura aqui é em pedaços, com prazo de parede e teto
    de tamanho.
    """
    if espacar:
        _aguardar_vez(urllib.parse.urlparse(url).netloc.lower())
    cab = {"User-Agent": ua, "Accept-Encoding": "gzip", "Accept-Language": "pt-BR,pt;q=0.9"}
    cab.update(cabecalhos or {})
    req = urllib.request.Request(url, data=dados, headers=cab)
    limite = time.monotonic() + prazo_total
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            pedacos, tamanho = [], 0
            while True:
                if time.monotonic() > limite:
                    return 0, b""                     # devagar demais: desiste e segue
                pedaco = r.read(65536)
                if not pedaco:
                    break
                pedacos.append(pedaco)
                tamanho += len(pedaco)
                if tamanho > MAX_CORPO:
                    break
            corpo = b"".join(pedacos)
            if r.headers.get("Content-Encoding") == "gzip":
                try:
                    corpo = gzip.decompress(corpo)
                except Exception:
                    # corpo truncado no teto: aproveita o que deu para descomprimir
                    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    corpo = d.decompress(corpo)
            return r.status, corpo
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception:
        return 0, b""


# ------------------------------------------------------------------- modelo
@dataclass
class Bruto:
    """Item cru do feed, antes de virar rd_item."""
    titulo: str
    gid: str
    url_gnews: str
    publicado_em: str | None
    veiculo_dominio: str | None
    veiculo_nome: str | None
    coletor: str
    expressao: str
    url_real: str | None = None
    capa_url: str | None = None
    resumo: str | None = None
    autor: str | None = None
    precisao: str = "aproximada"   # data do feed é do índice, não da matéria
    extras: dict = field(default_factory=dict)


# ------------------------------------------------------------- google news
def rss_google(expressao: str, janela: str = "30d") -> list[Bruto]:
    q = f"{expressao} when:{janela}" if janela else expressao
    url = f"{GNEWS_RSS}?" + urllib.parse.urlencode(
        {"q": q, "hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419"}
    )
    status, corpo = buscar(url)
    if status != 200 or not corpo:
        return []
    xml = corpo.decode("utf-8", "replace")
    itens: list[Bruto] = []
    for bloco in re.findall(r"<item>(.*?)</item>", xml, re.S):
        t = re.search(r"<title>(.*?)</title>", bloco, re.S)
        l = re.search(r"<link>(.*?)</link>", bloco, re.S)
        d = re.search(r"<pubDate>(.*?)</pubDate>", bloco, re.S)
        s = re.search(r'<source url="(.*?)"[^>]*>(.*?)</source>', bloco, re.S)
        if not (t and l):
            continue
        titulo = _html.unescape(t.group(1)).strip()
        # o Google concatena " - Veículo" no título; o veículo já vem em <source>
        if s:
            nome_v = _html.unescape(s.group(2)).strip()
            if titulo.endswith(f" - {nome_v}"):
                titulo = titulo[: -len(nome_v) - 3].strip()
        link = _html.unescape(l.group(1)).strip()
        gid = link.rsplit("/", 1)[-1].split("?")[0]
        quando = None
        if d:
            try:
                quando = parsedate_to_datetime(_html.unescape(d.group(1)).strip()).astimezone().isoformat()
            except Exception:
                quando = None
        dom = None
        if s:
            dom = urllib.parse.urlparse(s.group(1)).netloc.lower().removeprefix("www.")
        itens.append(Bruto(
            titulo=titulo, gid=gid, url_gnews=link, publicado_em=quando,
            veiculo_dominio=dom, veiculo_nome=_html.unescape(s.group(2)).strip() if s else None,
            coletor="google_news", expressao=expressao,
        ))
    return itens


def _assinatura(gid: str) -> tuple[str, str] | None:
    """Pega (timestamp, assinatura) da página do artigo — é o que autoriza o batchexecute."""
    status, corpo = buscar(f"https://news.google.com/rss/articles/{gid}?oc=5", timeout=25)
    if status != 200 or not corpo:
        return None
    pagina = corpo.decode("utf-8", "replace")
    m = re.search(r",(\d{10}),&quot;(A[\w-]{20,})&quot;", pagina)
    return (m.group(1), m.group(2)) if m else None


def _resolver_lote(triplas: list[tuple[str, str, str]]) -> dict[str, str]:
    """Resolve várias URLs numa única chamada. Uma requisição por lote, não por item."""
    if not triplas:
        return {}
    envelopes = []
    for gid, ts, sig in triplas:
        pedido = ["garturlreq",
                  [["X", "X", ["X", "X"], None, None, 1, 1, "US:en", None, 1,
                    None, None, None, None, None, 0, 1],
                   "X", "X", 1, [1, 1, 1], 1, 1, None, 0, 0, None, 0],
                  gid, int(ts), sig]
        envelopes.append(["Fbv4je", json.dumps(pedido), None, str(len(envelopes) + 1)])
    corpo_req = "f.req=" + urllib.parse.quote(json.dumps([envelopes]))
    status, corpo = buscar(
        GNEWS_BATCH, dados=corpo_req.encode(), timeout=45,
        cabecalhos={"Content-Type": "application/x-www-form-urlencoded;charset=utf-8"},
    )
    if status != 200 or not corpo:
        return {}
    texto = corpo.decode("utf-8", "replace")
    if texto.startswith(")]}'"):                       # prefixo anti-sequestro de JSON
        texto = texto[texto.find("\n"):]

    # O LOTE VOLTA FORA DE ORDEM. Cada envelope carrega o índice do pedido no
    # último campo — casar por posição embaralha URL, resumo e capa entre as
    # matérias, e o card passa a linkar a notícia errada. Foi exatamente o que
    # aconteceu na primeira rodada. Casar por índice, sempre.
    saida: dict[str, str] = {}
    for m in re.finditer(
        r'\["wrb\.fr","Fbv4je","((?:[^"\\]|\\.)*)",null,null,null,"(\d+)"\]', texto
    ):
        try:
            interno = json.loads('"' + m.group(1) + '"')   # desescapa a string aninhada
            dado = json.loads(interno)
        except Exception:
            continue
        url = dado[1] if isinstance(dado, list) and len(dado) > 1 else None
        i = int(m.group(2)) - 1                            # o índice é 1-based
        if url and isinstance(url, str) and url.startswith("http") and 0 <= i < len(triplas):
            saida[triplas[i][0]] = url
    return saida


def confere_url(titulo: str, url: str) -> bool:
    """A URL resolvida parece ser desta matéria?

    Guarda barata contra desalinhamento de lote: se o caminho da URL tem
    palavras e nenhuma delas aparece no título, quase certamente é a matéria de
    outro item. URL só com identificador numérico passa — não dá para afirmar
    nada sobre ela, e afirmar seria pior.
    """
    caminho = urllib.parse.urlparse(url).path.lower()
    do_url = {p for p in re.split(r"[^a-z0-9]+", caminho) if len(p) > 3 and not p.isdigit()}
    do_url -= {"materia", "noticia", "noticias", "html", "index", "cidades", "geral", "www"}
    if len(do_url) < 3:
        return True
    do_titulo = set(normalizar_titulo(titulo))
    do_url_sem_acento = {
        "".join(c for c in unicodedata.normalize("NFD", p) if unicodedata.category(c) != "Mn")
        for p in do_url
    }
    return bool(do_url_sem_acento & do_titulo)


def resolver_urls(itens: list[Bruto], *, paralelo: int = 8, lote: int = 15) -> int:
    """Preenche url_real. Devolve quantos resolveu."""
    pendentes = [i for i in itens if not i.url_real]
    if not pendentes:
        return 0
    with ThreadPoolExecutor(max_workers=paralelo) as pool:
        assinaturas = list(pool.map(lambda b: _assinatura(b.gid), pendentes))

    triplas, indice = [], {}
    for bruto, ass in zip(pendentes, assinaturas):
        if ass:
            triplas.append((bruto.gid, ass[0], ass[1]))
            indice[bruto.gid] = bruto

    resolvidos = torto = 0
    for i in range(0, len(triplas), lote):
        for gid, url in _resolver_lote(triplas[i:i + lote]).items():
            bruto = indice.get(gid)
            if not bruto:
                continue
            if not confere_url(bruto.titulo, url):
                # não aproveita URL suspeita: melhor o item ficar sem link do que
                # o card levar o leitor para outra matéria
                torto += 1
                bruto.extras["url_suspeita"] = url
                continue
            bruto.url_real = url
            resolvidos += 1
    if torto:
        print(f"    ⚠ {torto} URLs descartadas por não conferir com o título")
    return resolvidos


# ------------------------------------------------------------- rss nativo
def rss_nativo(feed_url: str) -> list[Bruto]:
    """Feed próprio do portal. Independe do Google — é a camada de garantia."""
    status, corpo = buscar(feed_url, ua=UA_ARTIGO, timeout=25)
    if status != 200 or not corpo:
        return []
    xml = corpo.decode("utf-8", "replace")
    dominio = urllib.parse.urlparse(feed_url).netloc.lower().removeprefix("www.")
    itens: list[Bruto] = []
    blocos = re.findall(r"<item>(.*?)</item>", xml, re.S) or re.findall(r"<entry>(.*?)</entry>", xml, re.S)
    for bloco in blocos:
        t = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", bloco, re.S)
        l = (re.search(r"<link[^>]*>(?:<!\[CDATA\[)?(https?://.*?)(?:\]\]>)?</link>", bloco, re.S)
             or re.search(r'<link[^>]*href="(https?://[^"]+)"', bloco))
        d = (re.search(r"<pubDate>(.*?)</pubDate>", bloco, re.S)
             or re.search(r"<published>(.*?)</published>", bloco, re.S)
             or re.search(r"<updated>(.*?)</updated>", bloco, re.S))
        if not (t and l):
            continue
        quando = None
        if d:
            cru = _html.unescape(d.group(1)).strip()
            try:
                quando = parsedate_to_datetime(cru).astimezone().isoformat()
            except Exception:
                try:
                    quando = datetime.fromisoformat(cru.replace("Z", "+00:00")).astimezone().isoformat()
                except Exception:
                    quando = None
        url = _html.unescape(l.group(1)).strip()
        itens.append(Bruto(
            titulo=_html.unescape(t.group(1)).strip(), gid=url, url_gnews=url,
            publicado_em=quando, veiculo_dominio=dominio, veiculo_nome=None,
            coletor="rss_nativo", expressao=feed_url, url_real=url,
            precisao="exata" if quando else "desconhecida",
        ))
    return itens


# ------------------------------------------------------------- normalização
def canonicalizar(url: str) -> str:
    """URL comparável. Sem isto o dedupe duro não pega replicação com utm."""
    try:
        p = urllib.parse.urlsplit(url.strip())
    except Exception:
        return url.strip().lower()
    esquema = "https"
    host = (p.netloc or "").lower().removeprefix("www.").split(":")[0]
    caminho = re.sub(r"/(amp|amp\.html)$", "", p.path or "/").rstrip("/") or "/"
    query = [(k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=False)
             if k.lower() not in LIXO_QUERY]
    query.sort()
    return urllib.parse.urlunsplit(
        (esquema, host, caminho, urllib.parse.urlencode(query), "")
    )


_STOP = {"a", "o", "as", "os", "de", "da", "do", "das", "dos", "e", "em", "no", "na",
         "nos", "nas", "um", "uma", "para", "por", "com", "que", "ao", "aos", "se",
         "the", "of", "to", "in", "on", "at"}


def normalizar_titulo(t: str) -> list[str]:
    """Minúscula, sem acento, sem pontuação, sem stopword.

    O acento sai por PROPRIEDADE Unicode (categoria Mn), nunca por caractere
    combinante literal — literal é a armadilha de charset que só quebra em
    produção, onde o servidor serve sem declarar o encoding.
    """
    t = unicodedata.normalize("NFD", t.lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    palavras = re.findall(r"[a-z0-9]+", t)
    return [p for p in palavras if p not in _STOP and len(p) > 1]


def simhash(titulo: str, bits: int = 64) -> int:
    """Simhash de shingles de 3 palavras. Títulos parecidos ficam a poucos bits."""
    palavras = normalizar_titulo(titulo)
    if not palavras:
        return 0
    shingles = ([" ".join(palavras[i:i + 3]) for i in range(max(1, len(palavras) - 2))]
                if len(palavras) >= 3 else palavras)
    vetor = [0] * bits
    for s in shingles:
        h = int.from_bytes(__import__("hashlib").blake2b(s.encode(), digest_size=8).digest(), "big")
        for i in range(bits):
            vetor[i] += 1 if (h >> i) & 1 else -1
    return sum(1 << i for i in range(bits) if vetor[i] > 0)


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


# Limiar de agrupamento de matéria, calibrado com IDF no acervo real de Itajaí:
#   · mesma matéria replicada .................. 0,23 a 1,00
#   · matérias de fato diferentes .............. 0,00 a 0,22
# As duas nuvens ENCOSTAM: não existe corte que acerte tudo. Em 0,30 não há
# nenhuma junção falsa, ao custo de deixar separados alguns pares que são a
# mesma notícia com fraseado muito distinto (ex.: "The Ocean Race deve gerar
# R$ 271 milhões" × "Itajaí estima impacto de R$ 270 milhões", 0,23).
# O erro fica na direção de SEPARAR DEMAIS, e isso é deliberado: juntar duas
# matérias distintas afirmaria uma replicação que não houve. A contagem de
# INSERÇÕES — que é a que o edital cobra — não depende disso e é exata.
LIMIAR_CLUSTER = 0.30
MIN_PALAVRAS_COMUNS = 3


def idf_do_acervo(titulos: list[str]) -> dict[str, float]:
    """Peso de raridade por palavra, medido no próprio acervo.

    Sem isso, nome de evento afunda o agrupamento: 'marina', 'itajai', 'boat' e
    'show' aparecem em dezenas de títulos e sozinhos já dão Jaccard alto entre
    matérias que não têm nada a ver — foi assim que 'Kawasaki acelera o mercado
    náutico', 'shopping náutico do Boat Show' e 'Boat Show cresce 20%' viraram
    uma matéria só. O que distingue é a palavra rara: 'kawasaki', 'sater'.
    """
    import math
    n = max(1, len(titulos))
    freq: dict[str, int] = {}
    for t in titulos:
        for p in set(normalizar_titulo(t)):
            freq[p] = freq.get(p, 0) + 1
    return {p: math.log(1 + n / f) for p, f in freq.items()}


def similaridade(titulo_a: str, titulo_b: str, idf: dict[str, float] | None = None) -> float:
    """Jaccard (ponderado por raridade, quando há IDF) sobre as palavras do título.

    Substituiu o simhash de shingles, que reprovou no acervo real: entre nove
    veículos publicando a MESMA matéria sobre o Festival de Música, a distância
    de Hamming ficou entre 20 e 37 bits — só título idêntico agrupava, e o
    resultado eram 117 inserções em 114 'matérias', o que esvazia justamente o
    indicador que o edital cobra. Shingle de 3 palavras é sensível demais a
    ordem e a fraseado; conjunto de palavras não é.
    """
    a, b = set(normalizar_titulo(titulo_a)), set(normalizar_titulo(titulo_b))
    if not a or not b:
        return 0.0
    comuns = a & b
    if len(comuns) < MIN_PALAVRAS_COMUNS:
        return 0.0          # título curto casa por acaso; exigir massa mínima
    if not idf:
        return len(comuns) / len(a | b)
    peso = lambda s: sum(idf.get(p, 1.0) for p in s)   # noqa: E731
    uniao = peso(a | b)
    return peso(comuns) / uniao if uniao else 0.0


def relevancia(titulo: str, resumo: str | None, cfg: dict) -> float:
    """Filtro de ruído regional.

    Uma busca por 'turismo Itajaí' devolve muita matéria de Balneário Camboriú e
    Navegantes. Sem este corte o wall enche de vizinho.
    """
    texto = f"{titulo} {resumo or ''}".lower()
    texto_sem_acento = "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )

    def tem(termo: str) -> bool:
        t = "".join(c for c in unicodedata.normalize("NFD", termo.lower())
                    if unicodedata.category(c) != "Mn")
        return t in texto_sem_acento

    if not any(tem(x) for x in cfg["exige_um_de"]):
        return 0.0
    nota = 1.0
    for ruido in cfg["penaliza"]:
        if tem(ruido):
            nota -= 0.30
    # menção no título vale mais que menção perdida no corpo
    if any(tem(x) for x in cfg["exige_um_de"] if x in titulo.lower()):
        nota += 0.15
    return max(0.0, min(1.0, nota))


def sem_acento(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", t.lower())
                   if unicodedata.category(c) != "Mn")


def eixos_de(titulo: str, resumo: str | None, corpo: str | None, cfg_tema: dict) -> list[str]:
    """Que eixos esta matéria toca.

    É o segundo filtro, e responde outra pergunta que a relevância: relevância
    diz 'é sobre Itajaí?', tema diz 'é sobre turismo e eventos?'. Release da
    Prefeitura sobre taekwondo passa no primeiro e reprova no segundo.

    Só olha TÍTULO e RESUMO, nunca o corpo. O corpo de um portal carrega menu,
    rodapé e chamadas de outras editorias — 'evento', 'show' e 'feira' aparecem
    lá em toda página, e foi assim que uma matéria de atropelamento na BR-101
    entrou no wall classificada como evento. Título e resumo são o que um humano
    lê para categorizar.

    O casamento é por limite de palavra: sem isso 'feira' casa dentro de
    'feirante' e 'porto' dentro de 'oportunidade'.
    """
    t = sem_acento(titulo)
    r = sem_acento(resumo or "")
    achados: list[tuple[str, float]] = []
    for slug, palavras in cfg_tema["palavras"].items():
        peso = 0.0
        for p in palavras:
            padrao = r"\b" + re.escape(sem_acento(p)) + r"\w{0,3}\b"
            if re.search(padrao, t):
                peso += 1.0
            elif re.search(padrao, r):
                peso += 0.5
        if peso >= 0.9:
            achados.append((slug, peso))
    achados.sort(key=lambda x: -x[1])
    return [s for s, _ in achados]


# -------------------------------------------------------- extração do artigo
def _jsonld(pagina: str) -> list[dict]:
    saida = []
    for bloco in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', pagina, re.S | re.I
    ):
        try:
            dado = json.loads(re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", bloco.strip()))
        except Exception:
            continue
        for d in (dado if isinstance(dado, list) else [dado]):
            if isinstance(d, dict):
                saida.extend(d.get("@graph", []) if "@graph" in d else [d])
    return [d for d in saida if isinstance(d, dict)]


def _meta(pagina: str, *chaves: str) -> str | None:
    """Lê uma meta tag.

    O casamento é fechado DENTRO da tag (`[^>]`) de propósito. Com `.*?` e
    re.S o padrão atravessa o fim da tag e varre o documento até achar a chave
    lá adiante — foi assim que o conteúdo do `<meta viewport>` acabou virando o
    resumo de várias matérias ("width=device-width, initial-scale=1…").
    """
    for chave in chaves:
        c = re.escape(chave)
        m = (re.search(rf'<meta[^>]*?(?:property|name)=["\']{c}["\'][^>]*?content=["\']([^"\']*)["\']',
                       pagina, re.I)
             or re.search(rf'<meta[^>]*?content=["\']([^"\']*)["\'][^>]*?(?:property|name)=["\']{c}["\']',
                          pagina, re.I))
        if m and m.group(1).strip():
            return _html.unescape(m.group(1)).strip()
    return None


def extrair_artigo(url: str) -> dict:
    """Abre a matéria e extrai data, manchete, resumo, capa e autor.

    JSON-LD primeiro (é o que a maioria dos portais brasileiros emite e é o
    dado que o próprio veículo declara), OpenGraph depois. Só isso já resolve
    o que o Firecrawl faria — que fica reservado para quem bloqueia.
    """
    status, corpo = buscar(url, ua=UA_ARTIGO, timeout=25)
    if status != 200 or not corpo:
        return {"_erro": f"http {status}", "_bloqueado": status in (403, 429, 401)}
    pagina = corpo.decode("utf-8", "replace")
    saida: dict = {}

    for d in _jsonld(pagina):
        tipo = d.get("@type", "")
        tipos = tipo if isinstance(tipo, list) else [tipo]
        if not any(str(t).endswith(("Article", "NewsArticle", "BlogPosting", "Report")) for t in tipos):
            continue
        saida.setdefault("publicado_em", d.get("datePublished") or d.get("dateCreated"))
        saida.setdefault("modificado_em", d.get("dateModified"))
        saida.setdefault("titulo", d.get("headline"))
        saida.setdefault("resumo", d.get("description"))
        img = d.get("image")
        if isinstance(img, dict):
            img = img.get("url")
        elif isinstance(img, list) and img:
            img = img[0].get("url") if isinstance(img[0], dict) else img[0]
        if img:
            saida.setdefault("capa_url", img)
        aut = d.get("author")
        if isinstance(aut, list) and aut:
            aut = aut[0]
        if isinstance(aut, dict):
            aut = aut.get("name")
        if aut:
            saida.setdefault("autor", aut)
        break

    saida.setdefault("publicado_em", _meta(pagina, "article:published_time", "publishdate", "date"))
    saida.setdefault("modificado_em", _meta(pagina, "article:modified_time"))
    saida.setdefault("titulo", _meta(pagina, "og:title", "twitter:title"))
    saida.setdefault("resumo", _meta(pagina, "og:description", "twitter:description", "description"))
    saida.setdefault("capa_url", _meta(pagina, "og:image", "og:image:secure_url", "twitter:image"))
    saida.setdefault("autor", _meta(pagina, "article:author", "author"))

    if saida.get("capa_url"):
        saida["capa_url"] = urllib.parse.urljoin(url, saida["capa_url"])

    # o corpo alimenta a validação de evidência literal da avaliação de humor
    texto = re.sub(r"<(script|style|nav|footer|aside)[^>]*>.*?</\1>", " ", pagina, flags=re.S | re.I)
    corpo_art = re.search(r"<article[^>]*>(.*?)</article>", texto, re.S | re.I)
    texto = corpo_art.group(1) if corpo_art else texto
    texto = _html.unescape(re.sub(r"<[^>]+>", " ", texto))
    saida["corpo"] = re.sub(r"\s+", " ", texto).strip()[:12000]

    return {k: v for k, v in saida.items() if v}


def normalizar_data(cru: str | None) -> tuple[str | None, str]:
    """Devolve (iso, precisao). Data do índice de busca não é data da matéria."""
    if not cru:
        return None, "desconhecida"
    cru = str(cru).strip()
    try:
        return datetime.fromisoformat(cru.replace("Z", "+00:00")).astimezone().isoformat(), "exata"
    except Exception:
        pass
    try:
        return parsedate_to_datetime(cru).astimezone().isoformat(), "exata"
    except Exception:
        pass
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", cru)
    if m:
        return datetime(*map(int, m.groups()), tzinfo=timezone.utc).astimezone().isoformat(), "exata"
    return None, "desconhecida"


def dentro_da_janela(iso: str | None, dias: int) -> bool:
    if not iso:
        return False
    try:
        quando = datetime.fromisoformat(iso)
    except Exception:
        return False
    if quando.tzinfo is None:
        quando = quando.replace(tzinfo=timezone.utc)
    return quando >= datetime.now(timezone.utc) - timedelta(days=dias)
