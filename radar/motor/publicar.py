"""Monta o payload do wall e copia as capas para a pasta publicável.

A metodologia não é escrita à mão: é derivada do banco. Se um feed falhou no
período, se um termo entrou depois, se X não é coberto — sai daqui, com o
número do período. Nota metodológica escrita à mão envelhece na primeira
semana e vira ficção.
"""
from __future__ import annotations

import json
import shutil
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from . import banco, capas, coleta


def _dia(iso: str | None) -> str | None:
    return iso[:10] if iso else None


def _faixa(n: int | None) -> str | None:
    if n is None:
        return None
    return "pos" if n >= 7 else "neu" if n >= 5 else "neg" if n >= 3 else "crit"


def _resumo(itens: list[dict], desde: date, ate: date, rotulo: str) -> dict:
    janela = [i for i in itens if i["d"] and desde.isoformat() <= i["d"][:10] <= ate.isoformat()]
    notas = [i["n"] for i in janela if i["n"] is not None]
    pos_neu = [n for n in notas if n >= 5]
    # destaque é o que teve mais replicação, não o que teve nota mais alta:
    # repercussão é o que interessa num clipping, não elogio
    destaques = sorted(janela, key=lambda i: (-(i.get("r") or 1), i["d"] or ""))[:4]
    criticas = sorted([i for i in janela if i["n"] is not None and i["n"] <= 4],
                      key=lambda i: i["n"])
    return {
        "periodo": rotulo,
        "insercoes": len(janela),
        "humor": round(sum(notas) / len(notas), 2) if notas else None,
        "pct_pos_neu": round(100 * len(pos_neu) / len(notas)) if notas else None,
        "destaques": [i["id"] for i in destaques],
        "atencao": criticas[0]["id"] if criticas else None,
    }


def montar_payload(con) -> dict:
    cliente = json.loads(con.execute("SELECT valor FROM rd_config WHERE chave='cliente'").fetchone()["valor"])
    limites_cfg = json.loads(con.execute("SELECT valor FROM rd_config WHERE chave='limites'").fetchone()["valor"])

    eixos = [dict(r) for r in con.execute(
        "SELECT slug, nome, cor_claro, cor_escuro, sensivel FROM rd_eixo WHERE ativo=1 ORDER BY ordem")]

    linhas = con.execute("""
        SELECT i.id, i.titulo, i.url, i.veiculo_id, i.publicado_em, i.publicado_precisao,
               i.capa_hash, i.autor, i.cluster_id, c.n_insercoes,
               a.nota, a.justificativa, a.polaridade, cp.lqip
          FROM rd_item i
          LEFT JOIN rd_cluster  c  ON c.id = i.cluster_id
          LEFT JOIN rd_avaliacao a ON a.id = i.avaliacao_id
          LEFT JOIN rd_capa     cp ON cp.hash = i.capa_hash
         WHERE i.estado <> 'descartado'
         ORDER BY i.publicado_em DESC
    """).fetchall()

    itens = [{
        "id": r["id"], "t": r["titulo"], "u": r["url"], "v": r["veiculo_id"],
        "d": r["publicado_em"], "p": r["publicado_precisao"],
        "c": r["capa_hash"], "l": r["lqip"], "a": r["autor"],
        "n": r["nota"], "j": r["justificativa"],
        "r": r["n_insercoes"] or 1,
        "e": [],
    } for r in linhas]
    por_id = {i["id"]: i for i in itens}

    for r in con.execute(
        "SELECT ie.item_id, e.slug FROM rd_item_eixo ie JOIN rd_eixo e ON e.id=ie.eixo_id"
    ):
        if r["item_id"] in por_id:
            por_id[r["item_id"]]["e"].append(r["slug"])

    usados = {i["v"] for i in itens if i["v"]}
    veiculos = [dict(r) for r in con.execute(
        "SELECT id, nome, dominio, tipo FROM rd_veiculo ORDER BY nome") if r["id"] in usados]

    # ---- série diária: o mapa de humor
    datas = [i["d"][:10] for i in itens if i["d"]]
    hoje = date.today()
    ini = date.fromisoformat(min(datas)) if datas else hoje - timedelta(days=29)
    fim = max(date.fromisoformat(max(datas)), hoje) if datas else hoje
    ini = max(ini, fim - timedelta(days=59))     # o mapa mostra no máximo 60 dias

    agrupado = defaultdict(list)
    for i in itens:
        if i["d"]:
            agrupado[i["d"][:10]].append(i)
    serie = []
    d = ini
    while d <= fim:
        chave = d.isoformat()
        doDia = agrupado.get(chave, [])
        notas = [x["n"] for x in doDia if x["n"] is not None]
        serie.append({"dia": chave, "n": len(doDia),
                      "humor": round(sum(notas) / len(notas), 2) if notas else None})
        d += timedelta(days=1)

    notas_todas = [i["n"] for i in itens if i["n"] is not None]
    pos_neu = [n for n in notas_todas if n >= 5]
    descartados = con.execute(
        "SELECT COUNT(*) c FROM rd_item WHERE estado='descartado'").fetchone()["c"]
    fora_geo = con.execute(
        "SELECT COUNT(*) c FROM rd_item WHERE estado='descartado' "
        "AND motivo_descarte LIKE 'relev%'").fetchone()["c"]
    fora_tema = con.execute(
        "SELECT COUNT(*) c FROM rd_item WHERE estado='descartado' "
        "AND motivo_descarte='fora de tema'").fetchone()["c"]

    rub = con.execute(
        "SELECT versao, titulo, modelo FROM rd_rubrica ORDER BY id DESC LIMIT 1").fetchone()
    rubrica = dict(rub) if rub else {"versao": "—", "titulo": "sem rubrica", "modelo": "—"}

    # ---- metodologia derivada do banco
    termos = [r["termo"] for r in con.execute(
        "SELECT termo FROM rd_termo WHERE vigente_ate IS NULL ORDER BY tipo, termo")]
    coletores = [f"{r['coletor']} ({r['n']})" for r in con.execute(
        "SELECT coletor, COUNT(*) n FROM rd_coleta GROUP BY coletor ORDER BY n DESC")]
    falhas = con.execute(
        "SELECT COUNT(*) c FROM rd_coleta WHERE status IN ('falha','parcial')").fetchone()["c"]
    aprox = sum(1 for i in itens if i["p"] != "exata")
    humanas = con.execute(
        "SELECT COUNT(*) c FROM rd_avaliacao WHERE origem='humano'").fetchone()["c"]

    # cada limite nomeia a plataforma: sem o rótulo a frase começa no meio e o
    # leitor não sabe do que ela fala
    ROTULO = {
        "x_twitter": "X/Twitter", "tiktok": "TikTok",
        "facebook_terceiros": "Facebook de terceiros", "instagram_sem_arroba": "Instagram",
        "chatgpt_perplexity": "ChatGPT e Perplexity",
    }
    limites = [
        f"{ROTULO[k]} — {limites_cfg[k]}"
        for k in ("x_twitter", "tiktok", "facebook_terceiros",
                  "instagram_sem_arroba", "chatgpt_perplexity")
    ] + [
        f"{fora_geo} itens varridos e descartados por não serem sobre Itajaí (a busca do Google "
        f"devolve muita cobertura de Balneário Camboriú e Navegantes).",
        f"{fora_tema} itens são sobre Itajaí mas não sobre turismo ou eventos (esporte, saúde, "
        f"segurança, serviço municipal) — cidade certa, assunto fora do escopo da SETUR.",
        "Os dois grupos ficam gravados com o motivo do descarte e podem ser reprocessados se o "
        "critério mudar: nenhum item varrido é perdido em silêncio.",
    ]
    if falhas:
        # os nomes saem do log de coleta, não de uma frase escrita à mão: um
        # limite que não acompanha o dado vira ficção na segunda semana
        mudas = [json.loads(r["params"] or "{}").get("feed", "")
                 for r in con.execute(
                     "SELECT params FROM rd_coleta WHERE status='falha' AND coletor='rss_nativo'")]
        mudas = sorted({m.split("//")[-1].split("/")[0] for m in mudas if m})
        detalhe = f" Sem resposta: {', '.join(mudas)}." if mudas else ""
        limites.append(f"{falhas} consultas falharam ou vieram parciais no período.{detalhe}")
    if aprox:
        limites.append(f"{aprox} inserções estão com data aproximada (veio do índice de busca, "
                       f"não da matéria) e aparecem marcadas com ~ no card.")
    # matéria sem nota não some do wall nem entra na conta como se fosse neutra:
    # aparece com "—" e é contada aqui, para o percentual do contrato ser lido
    # sabendo sobre que fatia ele foi calculado
    pendentes = len(itens) - len(notas_todas)
    if not notas_todas:
        limites.append("Nenhuma inserção classificada ainda: o humor não é exibido.")
    elif pendentes:
        limites.append(
            f"{pendentes} das {len(itens)} inserções ainda não têm nota de humor e aparecem "
            f"com “—” no card. O humor médio e o percentual positivo/neutro são calculados "
            f"apenas sobre as {len(notas_todas)} classificadas — não sobre o total."
        )

    # composição da amostra: sem isto o percentual positivo parece melhor do que é
    oficial = con.execute(
        "SELECT COUNT(*) c FROM rd_item i JOIN rd_veiculo v ON v.id=i.veiculo_id "
        "WHERE i.estado<>'descartado' AND v.tipo='oficial'").fetchone()["c"]
    if oficial and itens:
        limites.append(
            f"{oficial} das {len(itens)} inserções ({round(100*oficial/len(itens))}%) são "
            f"publicações do próprio órgão, não cobertura espontânea. Elas entram no wall "
            f"identificadas e com peso menor, mas puxam o percentual positivo para cima — "
            f"o indicador de favorabilidade deve ser lido com essa composição à vista."
        )
    limites.append(
        "O agrupamento de matéria usa similaridade de título ponderada por raridade de "
        "palavra, e erra de propósito para o lado de SEPARAR: nenhuma junção falsa, ao custo "
        "de manter separados alguns pares que são a mesma notícia com fraseado muito distinto. "
        "A contagem de inserções não depende do agrupamento e é exata."
    )

    return {
        "gerado_em": banco.agora(),
        "cliente": cliente,
        "periodo": {"ini": ini.isoformat(), "fim": fim.isoformat()},
        "rubrica": rubrica,
        "eixos": eixos,
        "veiculos": veiculos,
        "itens": itens,
        "serie_dias": serie,
        "kpi": {
            "insercoes": len(itens),
            "materias": con.execute(
                "SELECT COUNT(DISTINCT cluster_id) c FROM rd_item WHERE estado<>'descartado'"
            ).fetchone()["c"],
            "humor_medio": round(sum(notas_todas) / len(notas_todas), 2) if notas_todas else None,
            "pct_pos_neu": round(100 * len(pos_neu) / len(notas_todas)) if notas_todas else None,
            "criticas": sum(1 for n in notas_todas if n <= 4),
            "classificadas": len(notas_todas),
            "pendentes": pendentes,
            "descartados": descartados,
        },
        "resumo_semana": _resumo(itens, fim - timedelta(days=6), fim, "últimos 7 dias"),
        "resumo_mes": _resumo(itens, fim - timedelta(days=29), fim, "últimos 30 dias"),
        "metodologia": {
            "periodo": f"{ini.isoformat()} a {fim.isoformat()}",
            "fontes": "; ".join(coletores) or "—",
            "termos": ", ".join(termos),
            "rubrica": f"{rubrica['versao']} — {rubrica['titulo']}",
            "classificacao": (
                f"{len(notas_todas)} de {len(itens)} inserções classificadas. "
                f"{humanas} por leitura humana com evidência literal citada do próprio texto; "
                f"o percentual positivo/neutro usa o corte ≥5 declarado na rubrica."
            ),
            # descrito a partir das constantes do próprio código: nota metodológica
            # que afirma um método e o código roda outro é pior que nota nenhuma
            "dedupe": (
                "Dedupe duro por URL canônica (minúscula, https, sem www, sem utm/fbclid/gclid, "
                "sem sufixo AMP e com o redirect do Google News resolvido). Agrupamento de matéria "
                f"por similaridade de título ponderada pela raridade de cada palavra no acervo "
                f"(Jaccard com IDF), limiar {coleta.LIMIAR_CLUSTER:.2f}, mínimo de "
                f"{coleta.MIN_PALAVRAS_COMUNS} palavras em comum e janela de 72 horas. "
                "É o que separa inserção de matéria."
            ),
            "limites": limites,
        },
    }


def publicar(con, raiz: Path) -> dict:
    web = raiz / "web"
    payload = montar_payload(con)
    (web / "dados.json").write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    origem = raiz / "dados" / "capas"
    destino = web / "capas"
    copiadas = 0
    if origem.exists():
        for item in payload["itens"]:
            if not item["c"]:
                continue
            de = capas.caminho_de(origem, item["c"])
            para = capas.caminho_de(destino, item["c"])
            if de.exists() and not para.exists():
                para.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(de, para)
                copiadas += 1

    return {
        "itens": len(payload["itens"]),
        "capas_copiadas": copiadas,
        "bytes_json": (web / "dados.json").stat().st_size,
    }
