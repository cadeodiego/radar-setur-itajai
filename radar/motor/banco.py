"""Banco do RADAR.

O esquema mora num arquivo só (esquema.sql), escrito em subconjunto portável.
Aqui ele é traduzido para o dialeto do destino. A alternativa — manter dois
arquivos .sql — garante que eles divirjam silenciosamente na terceira semana.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
ESQUEMA = RAIZ / "esquema.sql"

DIALETOS = {
    "sqlite": {"{{PK}}": "INTEGER PRIMARY KEY AUTOINCREMENT", "{{ENGINE}}": ""},
    "mysql": {
        "{{PK}}": "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "{{ENGINE}}": " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
    },
}
VERSAO_ESQUEMA = 1


def agora() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def sql_para(dialeto: str) -> str:
    texto = ESQUEMA.read_text(encoding="utf-8")
    for marca, troca in DIALETOS[dialeto].items():
        texto = texto.replace(marca, troca)
    return texto


def abrir(caminho: Path) -> sqlite3.Connection:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(caminho)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    return con


def criar(con: sqlite3.Connection) -> None:
    """Cria o que falta. Idempotente — pode rodar em banco já povoado."""
    con.executescript(sql_para("sqlite"))
    ja = con.execute("SELECT COUNT(*) c FROM rd_schema_versao").fetchone()["c"]
    if not ja:
        con.execute(
            "INSERT INTO rd_schema_versao (versao, aplicado_em, nota) VALUES (?,?,?)",
            (VERSAO_ESQUEMA, agora(), "esquema inicial"),
        )
    con.commit()


def registrar(con: sqlite3.Connection, nivel: str, origem: str, msg: str) -> None:
    con.execute(
        "INSERT INTO rd_log (quando, nivel, origem, mensagem) VALUES (?,?,?,?)",
        (agora(), nivel, origem, msg),
    )


# ------------------------------------------------------------------- sementes
def semear(con: sqlite3.Connection) -> dict:
    """Carrega config/*.json para as tabelas. Não duplica se já rodou."""
    cfg = json.loads((RAIZ / "config" / "eixos.json").read_text(encoding="utf-8"))
    fontes = json.loads((RAIZ / "config" / "fontes.json").read_text(encoding="utf-8"))
    hoje = datetime.now().date().isoformat()

    for chave, valor in {
        "cliente": json.dumps(cfg["cliente"], ensure_ascii=False),
        "relevancia": json.dumps(cfg["relevancia"], ensure_ascii=False),
        "tema": json.dumps(cfg["tema"], ensure_ascii=False),
        "limites": json.dumps(fontes["fora_de_escopo"], ensure_ascii=False),
    }.items():
        con.execute(
            "INSERT INTO rd_config (chave, valor) VALUES (?,?) "
            "ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor",
            (chave, valor),
        )

    for e in cfg["eixos"]:
        con.execute(
            "INSERT INTO rd_eixo (slug, nome, cor_claro, cor_escuro, sensivel, ordem) "
            "VALUES (?,?,?,?,?,?) ON CONFLICT(slug) DO UPDATE SET "
            "nome=excluded.nome, cor_claro=excluded.cor_claro, cor_escuro=excluded.cor_escuro, "
            "sensivel=excluded.sensivel, ordem=excluded.ordem",
            (e["slug"], e["nome"], e["cor_claro"], e["cor_escuro"],
             1 if e.get("sensivel") else 0, e["ordem"]),
        )
        eixo_id = con.execute("SELECT id FROM rd_eixo WHERE slug=?", (e["slug"],)).fetchone()["id"]
        for q in e["consultas"]:
            existe = con.execute(
                "SELECT id FROM rd_consulta WHERE coletor='google_news' AND expressao=?", (q,)
            ).fetchone()
            if not existe:
                con.execute(
                    "INSERT INTO rd_consulta (coletor, eixo_id, expressao) VALUES ('google_news',?,?)",
                    (eixo_id, q),
                )

    # portais locais é COLETOR, não eixo: consulta com site: sem eixo fixo,
    # o eixo de cada matéria sai da classificação por termo
    pl = fontes["coletores"]["portais_locais"]
    if pl.get("ativo"):
        for site in pl["sites"]:
            for termo in pl["termos"]:
                expr = f"site:{site} {termo}"
                if not con.execute(
                    "SELECT id FROM rd_consulta WHERE coletor='portais_locais' AND expressao=?", (expr,)
                ).fetchone():
                    con.execute(
                        "INSERT INTO rd_consulta (coletor, eixo_id, expressao) VALUES ('portais_locais',NULL,?)",
                        (expr,),
                    )

    for t in cfg["termos"]:
        if not con.execute("SELECT id FROM rd_termo WHERE termo=?", (t["termo"],)).fetchone():
            con.execute(
                "INSERT INTO rd_termo (termo, tipo, variantes, obrigatorio, vigente_de) VALUES (?,?,?,?,?)",
                (t["termo"], t.get("tipo", "tema"),
                 json.dumps(t.get("variantes", []), ensure_ascii=False),
                 1 if t.get("obrigatorio") else 0, hoje),
            )

    for v in fontes["veiculos"]:
        con.execute(
            "INSERT INTO rd_veiculo (dominio, nome, tipo, uf, cidade, peso) VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(dominio) DO UPDATE SET nome=excluded.nome, tipo=excluded.tipo, peso=excluded.peso",
            (v["dominio"], v["nome"], v.get("tipo", "portal"), v.get("uf"), v.get("cidade"), v.get("peso", 1.0)),
        )

    _semear_rubrica(con)
    con.commit()
    return {
        "eixos": con.execute("SELECT COUNT(*) c FROM rd_eixo").fetchone()["c"],
        "consultas": con.execute("SELECT COUNT(*) c FROM rd_consulta").fetchone()["c"],
        "termos": con.execute("SELECT COUNT(*) c FROM rd_termo").fetchone()["c"],
        "veiculos": con.execute("SELECT COUNT(*) c FROM rd_veiculo").fetchone()["c"],
    }


PROMPT_V1 = """Você avalia a favorabilidade de uma matéria de imprensa à gestão pública
de {cliente}, na escala 0–10 da rubrica abaixo.

{rubrica}

Devolva JSON: nota (0-10), justificativa (UMA frase), evidencias (lista de trechos
COPIADOS LITERALMENTE do texto fornecido — nunca parafraseados, nunca inventados),
confianca (alta|media|baixa).

TÍTULO: {titulo}
VEÍCULO: {veiculo}
TEXTO: {texto}"""


def _semear_rubrica(con: sqlite3.Connection, modelo: str = "humano-fase1") -> int:
    """Garante que existe uma rubrica vigente para este classificador.

    O hash cobre texto + prompt + modelo + faixas. Mudou qualquer um deles, é
    **rubrica nova, com versão nova** — nunca edição da anterior. Sobrescrever
    reescreveria em silêncio o critério sob o qual as avaliações antigas foram
    feitas, e é exatamente o que a `mm_edicao` congelada existe para impedir.

    Devolve o id da rubrica correspondente ao modelo pedido.
    """
    texto = (RAIZ / "config" / "rubrica-v1.md").read_text(encoding="utf-8")
    faixas = json.dumps({
        "positivo": [7, 10], "neutro": [5, 6], "negativo": [0, 4],
        "meta_pos_neu": 75,
    })
    h = hashlib.sha256((texto + PROMPT_V1 + modelo + faixas).encode("utf-8")).hexdigest()

    ja = con.execute("SELECT id FROM rd_rubrica WHERE hash=?", (h,)).fetchone()
    if ja:
        return ja["id"]

    # versão nova: v1.0, v1.1, v1.2… conforme o critério ou o classificador mudam
    n = con.execute("SELECT COUNT(*) c FROM rd_rubrica").fetchone()["c"]
    versao = f"v1.{n}"
    con.execute(
        "INSERT INTO rd_rubrica (versao, titulo, texto_md, prompt_md, modelo, temperatura, "
        "faixas, hash, vigente_de, autor) VALUES (?,?,?,?,?,0,?,?,?,?)",
        (versao, "Favorabilidade ao governo — turismo e eventos", texto, PROMPT_V1,
         modelo, faixas, h, datetime.now().date().isoformat(), "/PLAN"),
    )
    return con.execute("SELECT id FROM rd_rubrica WHERE hash=?", (h,)).fetchone()["id"]


def veiculo_por_dominio(con: sqlite3.Connection, dominio: str, nome_alt: str | None = None) -> int:
    """Devolve o id do veículo, criando um registro raso se for domínio novo.

    Veículo desconhecido entra com peso 0.5: aparece no wall, mas não puxa a
    média de alcance para cima antes de alguém conferir quem é.
    """
    dominio = dominio.lower().removeprefix("www.")
    linha = con.execute("SELECT id FROM rd_veiculo WHERE dominio=?", (dominio,)).fetchone()
    if linha:
        return linha["id"]
    con.execute(
        "INSERT INTO rd_veiculo (dominio, nome, tipo, peso, ativo) VALUES (?,?,?,?,1)",
        (dominio, nome_alt or dominio, "portal", 0.5),
    )
    return con.execute("SELECT id FROM rd_veiculo WHERE dominio=?", (dominio,)).fetchone()["id"]
