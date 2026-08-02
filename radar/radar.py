#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pillow>=10.4"]
# ///
"""RADAR — monitoramento de mídia da /PLAN.

Uso:
    uv run radar.py semear                    cria o banco e carrega config/
    uv run radar.py coletar [--janela 30d]    varre as fontes
    uv run radar.py enriquecer [--limite N]   abre as matérias e extrai o dado
    uv run radar.py capas [--limite N]        baixa e rebaixa as miniaturas
    uv run radar.py estado                    o que tem no banco agora

Fase 1 roda no Mac de propósito: o plano prevê a primeira fase sem cadência
("disparo manual"). A cadência entra na Fase 2, no servidor.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from motor import banco, coleta  # noqa: E402

RAIZ = Path(__file__).resolve().parent
BANCO = RAIZ / "dados" / "radar.db"


def _con():
    con = banco.abrir(BANCO)
    banco.criar(con)
    return con


def _cfg(con, chave: str) -> dict:
    linha = con.execute("SELECT valor FROM rd_config WHERE chave=?", (chave,)).fetchone()
    return json.loads(linha["valor"]) if linha else {}


# ------------------------------------------------------------------ semear
def cmd_semear(_args) -> None:
    con = _con()
    contas = banco.semear(con)
    print("banco:", BANCO)
    for k, v in contas.items():
        print(f"  {k}: {v}")
    con.close()


# ----------------------------------------------------------------- coletar
JANELA_CLUSTER_H = 72


def _cluster_para(con, titulo: str, sim: int, publicado: str | None) -> int:
    """Dedupe mole: junta a mesma matéria replicada em veículos diferentes.

    O dedupe duro (URL canônica) não pega isso — e é justamente a replicação
    que o edital conta como inserção separada da matéria. Compara só dentro de
    uma janela de 72h: a mesma manchete seis meses depois é outra matéria.
    """
    if publicado:
        candidatos = con.execute(
            "SELECT id, cluster_id, titulo FROM rd_item "
            "WHERE cluster_id IS NOT NULL AND publicado_em IS NOT NULL "
            "AND ABS(JULIANDAY(publicado_em) - JULIANDAY(?)) <= ? LIMIT 400",
            (publicado, JANELA_CLUSTER_H / 24.0),
        ).fetchall()
    else:
        candidatos = []

    melhor, nota = None, 0.0
    for c in candidatos:
        s = coleta.similaridade(titulo, c["titulo"])
        if s >= coleta.LIMIAR_CLUSTER and s > nota:
            melhor, nota = c["cluster_id"], s
    if melhor:
        return melhor

    con.execute(
        "INSERT INTO rd_cluster (titulo_canonico, primeira_pub, ultima_pub, n_insercoes) "
        "VALUES (?,?,?,0)", (titulo, publicado, publicado),
    )
    return con.execute("SELECT last_insert_rowid() id").fetchone()["id"]


def _gravar(con, b: coleta.Bruto, coleta_id: int, cfg_rel: dict) -> str:
    """Grava um Bruto como rd_item. Devolve 'novo' | 'repetido' | 'ruido' | 'sem-url'."""
    url = b.url_real
    if not url:
        # Item cuja URL não resolveu, ou resolveu para algo que não confere com o
        # título. Fica gravado com o motivo: sumir com ele em silêncio é o mesmo
        # vício do descarte por relevância, e aqui a causa é nossa, não da fonte.
        motivo = ("url resolvida não confere com o título"
                  if b.extras.get("url_suspeita") else "url não resolvida")
        _gravar_descarte(con, b, coleta_id, 0.0, motivo)
        return "sem-url"

    canonica = coleta.canonicalizar(url)
    dedupe = hashlib.sha1(canonica.encode()).hexdigest()
    if con.execute("SELECT id FROM rd_item WHERE canal='imprensa' AND dedupe_hash=?", (dedupe,)).fetchone():
        return "repetido"

    rel = coleta.relevancia(b.titulo, b.resumo, cfg_rel)
    if rel < cfg_rel.get("corte", 0.35):
        return "ruido"

    sim = coleta.simhash(b.titulo)
    cluster_id = _cluster_para(con, b.titulo, sim, b.publicado_em)
    dominio = b.veiculo_dominio or coleta.urllib.parse.urlparse(url).netloc.lower().removeprefix("www.")
    veiculo_id = banco.veiculo_por_dominio(con, dominio, b.veiculo_nome)

    agora = banco.agora()
    con.execute(
        "INSERT INTO rd_item (canal, coletor, coleta_id, url, url_canonica, dedupe_hash, titulo, "
        "titulo_simhash, titulo_bucket, resumo, autor, veiculo_id, publicado_em, publicado_precisao, "
        "capa_url, cluster_id, relevancia, estado, bruto, criado_em, atualizado_em) "
        "VALUES ('imprensa',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'bruto',?,?,?)",
        (b.coletor, coleta_id, url, canonica, dedupe, b.titulo, str(sim), sim >> 48,
         b.resumo, b.autor, veiculo_id, b.publicado_em, b.precisao, b.capa_url,
         cluster_id, rel,
         json.dumps({"expressao": b.expressao, "gid": b.gid, "url_gnews": b.url_gnews},
                    ensure_ascii=False),
         agora, agora),
    )
    con.execute(
        "UPDATE rd_cluster SET n_insercoes=n_insercoes+1, "
        "primeira_pub=MIN(COALESCE(primeira_pub,?),?), ultima_pub=MAX(COALESCE(ultima_pub,?),?) "
        "WHERE id=?",
        (b.publicado_em, b.publicado_em, b.publicado_em, b.publicado_em, cluster_id),
    )
    item_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
    con.execute("INSERT INTO rd_fila (tarefa, item_id, prioridade) VALUES ('enriquecer',?,3)", (item_id,))
    return "novo"


def _gravar_descarte(con, b: coleta.Bruto, coleta_id: int, nota_rel: float,
                     motivo: str = "relevancia abaixo do corte") -> None:
    """Registra o que o filtro de relevância cortou, sem gastar resolução.

    Guarda o link do Google News mesmo (não resolvido) — serve de rastro, não
    de destino clicável. Se o corte for afrouxado depois, a lista já existe.
    """
    dedupe = hashlib.sha1(f"gnews:{b.gid}".encode()).hexdigest()
    if con.execute("SELECT id FROM rd_item WHERE canal='imprensa' AND dedupe_hash=?", (dedupe,)).fetchone():
        return
    dominio = b.veiculo_dominio or "desconhecido"
    agora = banco.agora()
    con.execute(
        "INSERT INTO rd_item (canal, coletor, coleta_id, url, url_canonica, dedupe_hash, titulo, "
        "resumo, veiculo_id, publicado_em, publicado_precisao, relevancia, estado, motivo_descarte, "
        "bruto, criado_em, atualizado_em) "
        "VALUES ('imprensa',?,?,?,?,?,?,?,?,?,?,?,'descartado',?,?,?,?)",
        (b.coletor, coleta_id, b.url_gnews, f"gnews:{b.gid}", dedupe, b.titulo, b.resumo,
         banco.veiculo_por_dominio(con, dominio, b.veiculo_nome), b.publicado_em, b.precisao,
         nota_rel, motivo,
         json.dumps({"expressao": b.expressao, "gid": b.gid,
                     "url_suspeita": b.extras.get("url_suspeita")}, ensure_ascii=False),
         agora, agora),
    )


def cmd_coletar(args) -> None:
    """Varre as fontes.

    A ordem importa por custo: resolver a URL real de um item do Google News
    custa duas requisições, e as 56 consultas se sobrepõem muito (a mesma
    matéria de Itajaí cai em vários eixos). Por isso a varredura junta tudo
    primeiro, deduplica por identificador e **descarta o ruído antes de
    resolver** — resolver matéria de Balneário Camboriú para depois jogar fora
    é o desperdício mais caro do pipeline.
    """
    con = _con()
    cfg_rel = _cfg(con, "relevancia")
    if not cfg_rel:
        print("banco sem sementes — rode: uv run radar.py semear")
        return

    consultas = con.execute(
        "SELECT id, coletor, expressao FROM rd_consulta WHERE ativa=1 ORDER BY coletor, id"
    ).fetchall()
    if args.coletor:
        consultas = [c for c in consultas if c["coletor"] == args.coletor]
    if args.limite_consultas:
        consultas = consultas[: args.limite_consultas]

    total = {"novo": 0, "repetido": 0, "ruido": 0, "sem-url": 0}

    # ---- passada 1: varrer (1 requisição por consulta) e juntar por gid
    por_gid: dict[str, coleta.Bruto] = {}
    coleta_de: dict[str, int] = {}
    for c in consultas:
        con.execute(
            "INSERT INTO rd_coleta (coletor, consulta_id, params, iniciado_em) VALUES (?,?,?,?)",
            (c["coletor"], c["id"], json.dumps({"janela": args.janela}), banco.agora()),
        )
        coleta_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        itens = coleta.rss_google(c["expressao"], args.janela)
        novos_gids = 0
        for b in itens:
            if b.gid not in por_gid:
                por_gid[b.gid] = b
                coleta_de[b.gid] = coleta_id
                novos_gids += 1
        con.execute(
            "UPDATE rd_coleta SET terminado_em=?, status=?, achados=? WHERE id=?",
            (banco.agora(), "ok" if itens else "falha", len(itens), coleta_id),
        )
        con.commit()
        print(f"  {c['coletor']:15} {c['expressao'][:50]:50} achou {len(itens):3} · inéditos {novos_gids:3}")

    # ---- passada 2: cortar ruído e o que já está no banco, ANTES de resolver
    # O descartado é GRAVADO, não sumido: descarte silencioso é o que faz um
    # relatório alegar cobertura que não tem. Fica como estado='descartado',
    # sem custo de resolução, auditável e pronto para ser reprocessado se o
    # corte mudar. O filtro só enxerga o título nesta altura, então erra para
    # o lado de cortar — e é por isso que ele precisa deixar rastro.
    candidatos = []
    for gid, b in por_gid.items():
        nota_rel = coleta.relevancia(b.titulo, b.resumo, cfg_rel)
        if nota_rel < cfg_rel.get("corte", 0.35):
            total["ruido"] += 1
            _gravar_descarte(con, b, coleta_de.get(gid, 0), nota_rel)
            continue
        candidatos.append(b)
    con.commit()
    if args.limite_itens:
        candidatos = candidatos[: args.limite_itens]

    print(f"\n  {len(por_gid)} itens únicos · {total['ruido']} fora de escopo · "
          f"{len(candidatos)} a resolver")

    # ---- passada 3: resolver a URL real só do que sobrou
    resolvidos = coleta.resolver_urls(candidatos) if candidatos else 0
    print(f"  resolvidos {resolvidos}/{len(candidatos)}")

    for b in candidatos:
        total[_gravar(con, b, coleta_de.get(b.gid, 0), cfg_rel)] += 1
    con.commit()

    # camada de garantia: feed próprio do portal, independente do Google
    if not args.coletor or args.coletor == "rss_nativo":
        fontes = json.loads((RAIZ / "config" / "fontes.json").read_text(encoding="utf-8"))
        rn = fontes["coletores"]["rss_nativo"]
        if rn.get("ativo"):
            for feed in rn["candidatos"]:
                con.execute(
                    "INSERT INTO rd_coleta (coletor, params, iniciado_em) VALUES ('rss_nativo',?,?)",
                    (json.dumps({"feed": feed}), banco.agora()),
                )
                coleta_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
                itens = coleta.rss_nativo(feed)
                conta = {"novo": 0, "repetido": 0, "ruido": 0, "sem-url": 0}
                for b in itens:
                    conta[_gravar(con, b, coleta_id, cfg_rel)] += 1
                for k, v in conta.items():
                    total[k] += v
                con.execute(
                    "UPDATE rd_coleta SET terminado_em=?, status=?, achados=?, novos=? WHERE id=?",
                    (banco.agora(), "ok" if itens else "falha", len(itens), conta["novo"], coleta_id),
                )
                con.commit()
                print(f"  {'rss_nativo':15} {feed[:52]:52} "
                      f"achou {len(itens):3} novos {conta['novo']:3} ruído {conta['ruido']:3}")

    print(f"\ntotal: {total['novo']} novos · {total['repetido']} repetidos · "
          f"{total['ruido']} fora de escopo · {total['sem-url']} sem URL resolvida")
    con.close()


# -------------------------------------------------------------- enriquecer
def _gravar_eixos(con, item_id: int, eixos: list[str]) -> None:
    """A confiança cai com a posição: o primeiro eixo é o assunto, o resto é toque."""
    for pos, slug in enumerate(eixos):
        eixo = con.execute("SELECT id FROM rd_eixo WHERE slug=?", (slug,)).fetchone()
        if eixo:
            con.execute(
                "INSERT INTO rd_item_eixo (item_id, eixo_id, confianca) VALUES (?,?,?) "
                "ON CONFLICT(item_id, eixo_id) DO NOTHING",
                (item_id, eixo["id"], round(max(0.2, 1.0 - pos * 0.2), 2)),
            )


def cmd_enriquecer(args) -> None:
    con = _con()
    # --retentar pega o que ficou sem corpo: quase sempre é o portal tendo
    # derrubado a conexão por excesso de pedido junto, não conteúdo ausente
    onde = ("i.estado='enriquecido' AND i.corpo IS NULL" if args.retentar
            else "i.estado='bruto'")
    fila = con.execute(
        f"SELECT i.id, i.url, i.titulo, i.resumo FROM rd_item i WHERE {onde} "
        "ORDER BY i.publicado_em DESC LIMIT ?", (args.limite,),
    ).fetchall()
    print(f"{len(fila)} matérias a abrir", flush=True)

    # abrir em paralelo, gravar em série e À MEDIDA que chega: se a rodada for
    # interrompida, o que já foi lido está no banco. Gravar tudo no fim faz uma
    # interrupção custar a rodada inteira.
    cfg_tema = _cfg(con, "tema")
    ok = bloqueadas = fora_de_tema = 0
    with ThreadPoolExecutor(max_workers=args.paralelo) as pool:
        futuros = {pool.submit(coleta.extrair_artigo, l["url"]): l for l in fila}
        for n, futuro in enumerate(as_completed(futuros), 1):
            linha = futuros[futuro]
            try:
                dado = futuro.result()
            except Exception as e:
                dado = {"_erro": type(e).__name__}
            if n % 25 == 0:
                print(f"  … {n}/{len(fila)}", flush=True)

            if dado.get("_erro"):
                bloqueadas += 1
                # Bloqueio não é matéria ruim: o item segue no wall com o dado do
                # feed, e a data fica marcada como aproximada em vez de inventada.
                # Mas o filtro de tema vale igual — senão um release bloqueado
                # sobre taekwondo entra no wall só por ter dado 403.
                eixos = coleta.eixos_de(linha["titulo"], linha["resumo"], None, cfg_tema)
                if not eixos and cfg_tema.get("descartar_sem_eixo"):
                    fora_de_tema += 1
                    con.execute(
                        "UPDATE rd_item SET estado='descartado', motivo_descarte='fora de tema', "
                        "atualizado_em=? WHERE id=?", (banco.agora(), linha["id"]),
                    )
                    con.commit()
                    continue
                _gravar_eixos(con, linha["id"], eixos)
                con.execute(
                    "UPDATE rd_item SET estado='enriquecido', atualizado_em=? WHERE id=?",
                    (banco.agora(), linha["id"]),
                )
                banco.registrar(con, "aviso", "enriquecer",
                                f"item {linha['id']} bloqueado: {dado['_erro']} — {linha['url'][:120]}")
                con.commit()
                continue

            # o eixo só pode ser decidido agora, com o texto na mão
            eixos = coleta.eixos_de(linha["titulo"], dado.get("resumo"),
                                    dado.get("corpo"), cfg_tema)
            if not eixos and cfg_tema.get("descartar_sem_eixo"):
                fora_de_tema += 1
                # guarda o que já foi extraído mesmo descartando: se o filtro de
                # tema for afinado depois, o item volta inteiro em vez de voltar
                # sem capa e sem texto
                publicado, precisao = coleta.normalizar_data(dado.get("publicado_em"))
                con.execute(
                    "UPDATE rd_item SET estado='descartado', motivo_descarte='fora de tema', "
                    "resumo=COALESCE(?,resumo), corpo=?, autor=COALESCE(?,autor), "
                    "capa_url=COALESCE(?,capa_url), publicado_em=COALESCE(?,publicado_em), "
                    "publicado_precisao=CASE WHEN ?='exata' THEN 'exata' ELSE publicado_precisao END, "
                    "atualizado_em=? WHERE id=?",
                    (dado.get("resumo"), dado.get("corpo"), dado.get("autor"),
                     dado.get("capa_url"), publicado, precisao, banco.agora(), linha["id"]),
                )
                con.commit()
                continue
            _gravar_eixos(con, linha["id"], eixos)

            publicado, precisao = coleta.normalizar_data(dado.get("publicado_em"))
            con.execute(
                "UPDATE rd_item SET resumo=COALESCE(?,resumo), corpo=?, autor=COALESCE(?,autor), "
                "capa_url=COALESCE(?,capa_url), publicado_em=COALESCE(?,publicado_em), "
                "publicado_precisao=CASE WHEN ?='exata' THEN 'exata' ELSE publicado_precisao END, "
                "estado='enriquecido', atualizado_em=? WHERE id=?",
                (dado.get("resumo"), dado.get("corpo"), dado.get("autor"), dado.get("capa_url"),
                 publicado, precisao, banco.agora(), linha["id"]),
            )
            if dado.get("capa_url"):
                con.execute("INSERT INTO rd_fila (tarefa, item_id, prioridade) VALUES ('capa',?,4)",
                            (linha["id"],))
            ok += 1
            con.commit()

    print(f"enriquecidas {ok} · bloqueadas {bloqueadas} (seguem no wall com o dado do feed) "
          f"· {fora_de_tema} fora de tema (cidade certa, assunto errado)")
    con.close()


# ------------------------------------------------------------- reclusterizar
def cmd_reclusterizar(_args) -> None:
    """Refaz o agrupamento de matérias no acervo inteiro.

    Usa união transitiva: se A junta com B e B junta com C, os três viram uma
    matéria só, mesmo que A e C não se pareçam diretamente. Sem isso a mesma
    notícia se parte em dois clusters conforme a ordem de chegada.
    """
    con = _con()
    itens = con.execute(
        "SELECT id, titulo, publicado_em FROM rd_item WHERE estado<>'descartado' "
        "ORDER BY publicado_em"
    ).fetchall()

    pai = {i["id"]: i["id"] for i in itens}

    def raiz(x):
        while pai[x] != x:
            pai[x] = pai[pai[x]]
            x = pai[x]
        return x

    janela = timedelta(hours=JANELA_CLUSTER_H)
    quando = {}
    for i in itens:
        try:
            quando[i["id"]] = datetime.fromisoformat(i["publicado_em"]) if i["publicado_em"] else None
        except Exception:
            quando[i["id"]] = None

    idf = coleta.idf_do_acervo([i["titulo"] for i in itens])
    pares = 0
    for a in range(len(itens)):
        ia = itens[a]
        for b in range(a + 1, len(itens)):
            ib = itens[b]
            qa, qb = quando[ia["id"]], quando[ib["id"]]
            if qa and qb:
                if abs(qa - qb) > janela:
                    if qb > qa:
                        break          # lista ordenada: daqui pra frente só piora
                    continue
            if coleta.similaridade(ia["titulo"], ib["titulo"], idf) >= coleta.LIMIAR_CLUSTER:
                ra, rb = raiz(ia["id"]), raiz(ib["id"])
                if ra != rb:
                    pai[rb] = ra
                    pares += 1

    grupos: dict[int, list[int]] = {}
    for i in itens:
        grupos.setdefault(raiz(i["id"]), []).append(i["id"])

    titulo_de = {i["id"]: i["titulo"] for i in itens}
    data_de = {i["id"]: i["publicado_em"] for i in itens}
    con.execute("DELETE FROM rd_cluster")
    for lider, membros in grupos.items():
        datas = sorted(d for d in (data_de[m] for m in membros) if d)
        con.execute(
            "INSERT INTO rd_cluster (titulo_canonico, primeira_pub, ultima_pub, n_insercoes) "
            "VALUES (?,?,?,?)",
            (titulo_de[lider], datas[0] if datas else None,
             datas[-1] if datas else None, len(membros)),
        )
        cid = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        for m in membros:
            con.execute("UPDATE rd_item SET cluster_id=? WHERE id=?", (cid, m))

    con.execute("""
        UPDATE rd_cluster SET nota_media = (
            SELECT AVG(a.nota) FROM rd_item i JOIN rd_avaliacao a ON a.id=i.avaliacao_id
             WHERE i.cluster_id = rd_cluster.id)
    """)
    con.commit()

    print(f"{len(itens)} inserções em {len(grupos)} matérias ({pares} junções)")
    print("  mais replicadas:")
    for r in con.execute(
        "SELECT titulo_canonico, n_insercoes FROM rd_cluster WHERE n_insercoes>1 "
        "ORDER BY n_insercoes DESC LIMIT 6"
    ):
        print(f"    {r['n_insercoes']}x  {r['titulo_canonico'][:62]}")
    con.close()


# -------------------------------------------------------------------- reeixos
def cmd_reeixos(_args) -> None:
    """Recalcula o eixo de cada item a partir do texto já guardado.

    Serve para afinar as palavras do filtro de tema sem rebaixar 200 páginas de
    novo. Ressuscita o descartado por tema antes de reavaliar — do contrário o
    ajuste só consegue cortar mais, nunca corrigir um corte errado.
    """
    con = _con()
    cfg_tema = _cfg(con, "tema")
    con.execute(
        "UPDATE rd_item SET estado='enriquecido', motivo_descarte=NULL "
        "WHERE estado='descartado' AND motivo_descarte='fora de tema'"
    )
    con.execute("DELETE FROM rd_item_eixo")

    linhas = con.execute(
        "SELECT id, titulo, resumo FROM rd_item WHERE estado IN ('enriquecido','classificado')"
    ).fetchall()
    dentro = fora = 0
    for l in linhas:
        eixos = coleta.eixos_de(l["titulo"], l["resumo"], None, cfg_tema)
        if not eixos and cfg_tema.get("descartar_sem_eixo"):
            con.execute(
                "UPDATE rd_item SET estado='descartado', motivo_descarte='fora de tema', "
                "atualizado_em=? WHERE id=?", (banco.agora(), l["id"]),
            )
            fora += 1
            continue
        _gravar_eixos(con, l["id"], eixos)
        dentro += 1
    con.commit()
    print(f"{dentro} no wall · {fora} fora de tema")
    for r in con.execute(
        "SELECT e.nome, COUNT(*) n FROM rd_item_eixo ie JOIN rd_eixo e ON e.id=ie.eixo_id "
        "GROUP BY e.id ORDER BY n DESC"
    ):
        print(f"    {r['nome'][:28]:28} {r['n']}")
    con.close()


# ---------------------------------------------------------------- reenriquecer
def cmd_reenriquecer(_args) -> None:
    """Volta todo item coletado ao estado bruto, para reprocessar a extração.

    Ressuscita também o que foi descartado POR TEMA: esse descarte é decidido
    com título e resumo, e se a extração do resumo estava errada, o julgamento
    também estava. O descarte por relevância geográfica não é tocado — ele usa
    só o título do feed, que não depende da extração.
    """
    con = _con()
    revividos = con.execute(
        "SELECT COUNT(*) c FROM rd_item WHERE estado='descartado' AND motivo_descarte='fora de tema'"
    ).fetchone()["c"]
    con.execute(
        "UPDATE rd_item SET estado='bruto', motivo_descarte=NULL, resumo=NULL, corpo=NULL, "
        "capa_url=NULL, capa_hash=NULL, capa_status='pendente' "
        "WHERE estado='descartado' AND motivo_descarte='fora de tema'"
    )
    afetados = con.execute(
        "SELECT COUNT(*) c FROM rd_item WHERE estado IN ('enriquecido','classificado')"
    ).fetchone()["c"]
    con.execute(
        "UPDATE rd_item SET estado='bruto', resumo=NULL, corpo=NULL, capa_url=NULL, "
        "capa_hash=NULL, capa_status='pendente', avaliacao_id=NULL "
        "WHERE estado IN ('enriquecido','classificado')"
    )
    con.execute("DELETE FROM rd_item_eixo")
    con.commit()
    print(f"{afetados} itens voltaram para bruto · {revividos} descartados por tema ressuscitados")
    print("agora rode: uv run radar.py enriquecer --limite 500")
    con.close()


# -------------------------------------------------------------------- capas
def cmd_capas(args) -> None:
    from motor import capas as mod_capas

    con = _con()
    raiz = RAIZ / "dados" / "capas"
    fila = con.execute(
        "SELECT id, capa_url FROM rd_item WHERE estado<>'descartado' AND capa_status='pendente' "
        "AND capa_url IS NOT NULL ORDER BY publicado_em DESC LIMIT ?", (args.limite,),
    ).fetchall()
    print(f"{len(fila)} capas a baixar")

    with ThreadPoolExecutor(max_workers=args.paralelo) as pool:
        saidas = list(pool.map(lambda l: mod_capas.processar(l["capa_url"], raiz), fila))

    ok = falhou = 0
    for linha, meta in zip(fila, saidas):
        if not meta or meta.get("_erro"):
            con.execute(
                "UPDATE rd_item SET capa_status='falha', atualizado_em=? WHERE id=?",
                (banco.agora(), linha["id"]),
            )
            banco.registrar(con, "aviso", "capas",
                            f"item {linha['id']}: {(meta or {}).get('_erro', 'sem retorno')}")
            falhou += 1
            continue
        con.execute(
            "INSERT INTO rd_capa (hash, largura, altura, bytes, mime, origem_url, lqip, baixado_em, formato) "
            "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(hash) DO NOTHING",
            (meta["hash"], meta["largura"], meta["altura"], meta["bytes"], meta["mime"],
             meta["origem_url"], meta["lqip"], banco.agora(), meta["formato"]),
        )
        con.execute(
            "UPDATE rd_item SET capa_hash=?, capa_status='ok', atualizado_em=? WHERE id=?",
            (meta["hash"], banco.agora(), linha["id"]),
        )
        ok += 1
    con.commit()
    # falha aqui não é buraco no wall: quem não tem capa recebe cartão
    # tipográfico montado em CSS pela própria página
    print(f"baixadas {ok} · sem capa utilizável {falhou} (viram cartão tipográfico no wall)")
    con.close()


# ------------------------------------------------------------ classificar
def _polaridade(nota: int) -> str:
    return "positivo" if nota >= 7 else "neutro" if nota >= 5 else "negativo"


def _faixa_nome(nota: int) -> str:
    return ("Muito favorável" if nota >= 9 else "Favorável" if nota >= 7
            else "Misto/neutro" if nota >= 5 else "Desfavorável" if nota >= 3
            else "Muito desfavorável")


def cmd_classificar(args) -> None:
    """Importa avaliações e VALIDA cada evidência contra o texto do próprio item.

    A validação é o que separa avaliação auditável de opinião de caixa-preta:
    se o trecho citado não existe literalmente no texto avaliado, a avaliação
    entra com confiança baixa e fica marcada para revisão — em vez de virar
    número bonito num relatório.
    """
    con = _con()
    rubrica = con.execute("SELECT id, versao, modelo FROM rd_rubrica ORDER BY id DESC LIMIT 1").fetchone()
    if not rubrica:
        print("sem rubrica no banco — rode: uv run radar.py semear")
        return

    entradas = json.loads(Path(args.arquivo).read_text(encoding="utf-8"))
    ok = sem_evidencia = nao_achou = 0

    for e in entradas:
        linha = con.execute(
            "SELECT titulo, resumo, corpo FROM rd_item WHERE id=?", (e["id"],)
        ).fetchone()
        if not linha:
            continue
        texto = " ".join(filter(None, [linha["titulo"], linha["resumo"], linha["corpo"]])).lower()

        evidencias = e.get("evidencias") or []
        validas = [t for t in evidencias if t and t.lower()[:70] in texto]
        if not evidencias:
            confianca, sem_evidencia = "baixa", sem_evidencia + 1
        elif len(validas) < len(evidencias):
            confianca, nao_achou = "baixa", nao_achou + 1
        else:
            confianca = e.get("confianca", "media")

        nota = int(e["nota"])
        con.execute(
            "INSERT INTO rd_avaliacao (item_id, rubrica_id, nota, faixa, polaridade, justificativa, "
            "evidencias, confianca, modelo, origem, criado_em) VALUES (?,?,?,?,?,?,?,?,?,'humano',?)",
            (e["id"], rubrica["id"], nota, _faixa_nome(nota), _polaridade(nota),
             e.get("justificativa"), json.dumps(validas, ensure_ascii=False),
             confianca, rubrica["modelo"], banco.agora()),
        )
        aval_id = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        con.execute(
            "UPDATE rd_item SET avaliacao_id=?, estado='classificado', atualizado_em=? WHERE id=?",
            (aval_id, banco.agora(), e["id"]),
        )
        ok += 1

    # nota média do cluster: é o que o resumo por matéria usa
    con.execute("""
        UPDATE rd_cluster SET nota_media = (
            SELECT AVG(a.nota) FROM rd_item i JOIN rd_avaliacao a ON a.id=i.avaliacao_id
             WHERE i.cluster_id = rd_cluster.id)
    """)
    con.commit()
    print(f"classificadas {ok}")
    if sem_evidencia:
        print(f"  {sem_evidencia} sem evidência citada → confiança baixa")
    if nao_achou:
        print(f"  {nao_achou} com trecho que não confere com o texto → confiança baixa, revisar")
    con.close()


def cmd_pendentes(args) -> None:
    """Despeja o que falta classificar, no formato de entrada do classificar."""
    con = _con()
    linhas = con.execute(
        "SELECT i.id, i.titulo, i.resumo, v.nome veiculo, i.publicado_em "
        "FROM rd_item i LEFT JOIN rd_veiculo v ON v.id=i.veiculo_id "
        "WHERE i.estado<>'descartado' AND i.avaliacao_id IS NULL "
        "ORDER BY i.publicado_em DESC LIMIT ?", (args.limite,),
    ).fetchall()
    saida = [{
        "id": r["id"], "veiculo": r["veiculo"], "data": (r["publicado_em"] or "")[:10],
        "titulo": r["titulo"], "resumo": (r["resumo"] or "")[:400],
    } for r in linhas]
    Path(args.saida).write_text(json.dumps(saida, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{len(saida)} pendentes em {args.saida}")
    con.close()


# ----------------------------------------------------------- autoclassificar
def cmd_autoclassificar(args) -> None:
    """Classifica o que chegou de novo, pela API, com a mesma rubrica.

    A rubrica é versionada por classificador: a v1.0 é a leitura humana da
    Fase 1, e a passagem para a API cria uma versão nova em vez de reescrever
    a anterior. Assim o relatório de julho continua explicável por quem o
    produziu, mesmo depois de a operação virar automática.
    """
    from motor import classificar as mod_cls

    con = _con()
    fila = con.execute(
        "SELECT i.id, i.titulo, i.resumo, i.corpo, v.nome veiculo FROM rd_item i "
        "LEFT JOIN rd_veiculo v ON v.id=i.veiculo_id "
        "WHERE i.estado<>'descartado' AND i.avaliacao_id IS NULL "
        "ORDER BY i.publicado_em DESC LIMIT ?", (args.limite,),
    ).fetchall()
    if not fila:
        print("nada a classificar")
        return

    # A chave só é exigida quando há trabalho: cobrar antes reprovaria uma
    # rodada que não tinha nada a fazer. Mas é conferida ANTES de disparar as
    # threads — senão o erro voltaria como traceback, um por item.
    try:
        mod_cls._chave()
    except mod_cls.SemChave as e:
        print(f"ERRO: {e}")
        sys.exit(1)

    cliente = _cfg(con, "cliente").get("nome", "o município")
    rubrica_id = banco._semear_rubrica(con, modelo=args.modelo)
    con.commit()
    rub = con.execute(
        "SELECT id, versao, texto_md, prompt_md, modelo FROM rd_rubrica WHERE id=?", (rubrica_id,)
    ).fetchone()
    print(f"{len(fila)} a classificar · rubrica {rub['versao']} · modelo {rub['modelo']}", flush=True)

    def trabalho(l):
        return l, mod_cls.avaliar(dict(l), rub["texto_md"], rub["prompt_md"], cliente, args.modelo)

    ok = falha = rebaixadas = 0
    custo = 0.0
    with ThreadPoolExecutor(max_workers=args.paralelo) as pool:
        for linha, r in pool.map(trabalho, fila):
            if r.get("_erro"):
                falha += 1
                banco.registrar(con, "erro", "autoclassificar",
                                f"item {linha['id']}: {r['_erro']}")
                continue

            texto = " ".join(filter(None, [linha["titulo"], linha["resumo"], linha["corpo"]])).lower()
            evid = r.get("evidencias") or []
            validas = [t for t in evid if t and t.lower()[:70] in texto]
            if not evid or len(validas) < len(evid):
                confianca, rebaixadas = "baixa", rebaixadas + 1
            else:
                confianca = r.get("confianca", "media")

            nota = max(0, min(10, int(r["nota"])))
            con.execute(
                "INSERT INTO rd_avaliacao (item_id, rubrica_id, nota, faixa, polaridade, "
                "justificativa, evidencias, confianca, modelo, tokens_in, tokens_out, custo_usd, "
                "origem, criado_em) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'auto',?)",
                (linha["id"], rub["id"], nota, _faixa_nome(nota), _polaridade(nota),
                 (r.get("justificativa") or "")[:400], json.dumps(validas, ensure_ascii=False),
                 confianca, r["_modelo"], r["_tokens_in"], r["_tokens_out"], r["_custo_usd"],
                 banco.agora()),
            )
            aval = con.execute("SELECT last_insert_rowid() id").fetchone()["id"]
            con.execute(
                "UPDATE rd_item SET avaliacao_id=?, estado='classificado', atualizado_em=? WHERE id=?",
                (aval, banco.agora(), linha["id"]),
            )
            custo += r["_custo_usd"]
            ok += 1
            con.commit()

    con.commit()
    print(f"classificadas {ok} · falhas {falha} · custo US$ {custo:.4f}")
    if rebaixadas:
        print(f"  {rebaixadas} com evidência que não confere → confiança baixa, revisar")
    con.close()

    # Falhar alto. Publicar um painel cheio de matéria sem nota, com saída zero,
    # é pior que não publicar: o mapa de humor e o percentual do contrato ficam
    # errados e nada avisa.
    if falha and ok == 0:
        print("ERRO: nenhuma classificação concluiu — não seguir para a publicação.")
        sys.exit(1)
    if falha > ok:
        print(f"ERRO: mais falhas ({falha}) que sucessos ({ok}) — rodada não confiável.")
        sys.exit(1)


# ---------------------------------------------------------------- compactar
def cmd_compactar(args) -> None:
    """Mantém o repositório pequeno o bastante para viver no git.

    Duas podas, ambas sem perder o que o relatório precisa: o corpo da matéria
    só serve para validar evidência no momento da classificação — depois disso
    é peso morto; e o wall guarda uma janela móvel, não o acervo eterno.
    """
    from motor import capas as mod_capas

    con = _con()
    limite_corpo = (datetime.now() - timedelta(days=args.dias_corpo)).isoformat()
    n = con.execute(
        "SELECT COUNT(*) c FROM rd_item WHERE corpo IS NOT NULL AND publicado_em < ?",
        (limite_corpo,),
    ).fetchone()["c"]
    con.execute("UPDATE rd_item SET corpo=NULL WHERE corpo IS NOT NULL AND publicado_em < ?",
                (limite_corpo,))

    limite_janela = (datetime.now() - timedelta(days=args.dias_janela)).isoformat()
    velhos = con.execute(
        "SELECT id, capa_hash FROM rd_item WHERE publicado_em < ? AND estado<>'arquivado'",
        (limite_janela,),
    ).fetchall()
    apagadas = 0
    vivos = {r["capa_hash"] for r in con.execute(
        "SELECT DISTINCT capa_hash FROM rd_item WHERE capa_hash IS NOT NULL "
        "AND (publicado_em >= ? OR publicado_em IS NULL)", (limite_janela,))}
    for r in velhos:
        if r["capa_hash"] and r["capa_hash"] not in vivos:
            for raiz in (RAIZ / "dados" / "capas", RAIZ / "web" / "capas"):
                p = mod_capas.caminho_de(raiz, r["capa_hash"])
                if p.exists():
                    p.unlink()
                    apagadas += 1
            con.execute("DELETE FROM rd_capa WHERE hash=?", (r["capa_hash"],))
            con.execute("UPDATE rd_item SET capa_hash=NULL, capa_status='ausente' WHERE id=?",
                        (r["id"],))
    con.commit()
    con.execute("VACUUM")
    tam = BANCO.stat().st_size / 1024 / 1024
    print(f"corpo limpo em {n} itens (>{args.dias_corpo}d) · {apagadas} arquivos de capa "
          f"removidos (>{args.dias_janela}d) · banco {tam:.1f} MB")
    con.close()


# --------------------------------------------------------- exportar/importar
# Tabelas cujo conteúdo precisa sobreviver entre rodadas. Ficam de fora as de
# operação (fila, log, coleta), que são refeitas a cada passada.
TABELAS_DURAVEIS = [
    "rd_config", "rd_veiculo", "rd_eixo", "rd_termo", "rd_consulta", "rd_cluster",
    "rd_item", "rd_item_eixo", "rd_item_termo", "rd_metrica", "rd_rubrica",
    "rd_avaliacao", "rd_rubrica_calibragem", "rd_numero", "rd_edicao",
    "rd_edicao_item", "rd_capa",
]
ACERVO = "dados/acervo.jsonl"


def cmd_exportar(args) -> None:
    """Escreve o acervo como JSONL — o registro durável do produto.

    O banco é grande e binário: commitado todo dia viraria centenas de MB de
    histórico por ano, e o git não sabe fazer delta de SQLite. O JSONL é texto,
    versiona com diff legível e cabe em poucas centenas de KB. Na prática o
    banco passa a ser cache reconstruível, e ISTO é o acervo.

    O corpo da matéria não entra: serve para validar evidência na hora da
    classificação e depois é peso morto — a evidência citada já fica guardada
    na própria avaliação, e a URL original continua lá para quem quiser conferir.
    """
    con = _con()
    destino = Path(args.saida)
    destino.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with destino.open("w", encoding="utf-8") as f:
        for tabela in TABELAS_DURAVEIS:
            try:
                linhas = con.execute(f"SELECT * FROM {tabela}").fetchall()
            except Exception:
                continue
            for linha in linhas:
                d = dict(linha)
                d.pop("corpo", None)
                f.write(json.dumps({"_t": tabela, **d}, ensure_ascii=False) + "\n")
                n += 1
    tam = destino.stat().st_size / 1024
    print(f"{n} registros em {destino} ({tam:.0f} KB)")
    con.close()


def cmd_importar(args) -> None:
    """Reconstrói o banco a partir do JSONL. Usado quando o cache se perde."""
    origem = Path(args.arquivo)
    if not origem.exists():
        print(f"sem {origem} — nada a reconstruir")
        return
    con = _con()
    banco.semear(con)
    n = erros = 0
    for bruta in origem.read_text(encoding="utf-8").splitlines():
        if not bruta.strip():
            continue
        d = json.loads(bruta)
        tabela = d.pop("_t")
        campos = ",".join(d)
        marcas = ",".join("?" * len(d))
        try:
            con.execute(f"INSERT OR REPLACE INTO {tabela} ({campos}) VALUES ({marcas})",
                        tuple(d.values()))
            n += 1
        except Exception:
            erros += 1
    con.commit()
    print(f"{n} registros restaurados · {erros} recusados")
    con.close()


# ---------------------------------------------------------------- publicar
def cmd_publicar(_args) -> None:
    from motor import publicar as mod_pub

    con = _con()
    r = mod_pub.publicar(con, RAIZ)
    print(f"dados.json: {r['itens']} inserções · {r['bytes_json'] // 1024} KB")
    print(f"capas copiadas: {r['capas_copiadas']}")
    print(f"servir local:  cd web && python3 -m http.server 8765")
    con.close()


# ------------------------------------------------------------------ estado
def cmd_estado(_args) -> None:
    con = _con()

    # o corte por estado é o ponto: descartado não é inserção, e somar os dois
    # numa linha só é exatamente o vício que este produto existe para evitar
    NO_WALL = "WHERE estado<>'descartado'"

    def conta(extra: str = "") -> int:
        return con.execute(
            f"SELECT COUNT(*) c FROM rd_item {NO_WALL} {extra}").fetchone()["c"]

    clusters = con.execute("SELECT COUNT(*) c FROM rd_cluster").fetchone()["c"]
    total_varrido = con.execute("SELECT COUNT(*) c FROM rd_item").fetchone()["c"]
    print(f"banco: {BANCO}\n")
    print(f"  inserções no wall ....... {conta()}")
    print(f"  matérias (clusters) ..... {clusters}")
    print(f"  varridos ao todo ........ {total_varrido} (o resto foi descartado, com motivo)")
    com_capa = conta("AND capa_status='ok'")
    exatas = conta("AND publicado_precisao='exata'")
    print(f"  com capa baixada ........ {com_capa}")
    print(f"  data exata .............. {exatas}")
    print(f"  classificadas ........... {conta('AND avaliacao_id IS NOT NULL')}")
    print("\n  por estado:")
    for r in con.execute("SELECT estado, COUNT(*) c FROM rd_item GROUP BY estado ORDER BY c DESC"):
        print(f"    {r['estado']:14} {r['c']}")
    print("\n  top veículos no wall:")
    for r in con.execute(
        "SELECT v.nome, v.tipo, COUNT(*) c, ROUND(AVG(a.nota),1) m "
        "FROM rd_item i JOIN rd_veiculo v ON v.id=i.veiculo_id "
        "LEFT JOIN rd_avaliacao a ON a.id=i.avaliacao_id "
        "WHERE i.estado<>'descartado' GROUP BY v.id ORDER BY c DESC LIMIT 8"
    ):
        marca = " ← voz do próprio órgão" if r["tipo"] == "oficial" else ""
        print(f"    {r['nome'][:26]:26} {r['c']:3}  humor {r['m'] or '—'}{marca}")
    rep = con.execute(
        "SELECT c.titulo_canonico, c.n_insercoes FROM rd_cluster c WHERE c.n_insercoes>1 "
        "ORDER BY c.n_insercoes DESC LIMIT 5"
    ).fetchall()
    if rep:
        print("\n  matérias replicadas (a prova do dedupe mole):")
        for r in rep:
            print(f"    {r['n_insercoes']}x  {r['titulo_canonico'][:62]}")
    con.close()


def main() -> None:
    p = argparse.ArgumentParser(description="RADAR — monitoramento de mídia /PLAN")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("semear").set_defaults(f=cmd_semear)

    c = sub.add_parser("coletar")
    c.add_argument("--janela", default="30d")
    c.add_argument("--coletor", default=None)
    c.add_argument("--limite-consultas", type=int, default=0, help="teto de consultas na rodada")
    c.add_argument("--limite-itens", type=int, default=0, help="teto de itens a resolver na rodada")
    c.set_defaults(f=cmd_coletar)

    e = sub.add_parser("enriquecer")
    e.add_argument("--limite", type=int, default=100)
    e.add_argument("--paralelo", type=int, default=8)
    e.add_argument("--retentar", action="store_true", help="reprocessa o que ficou sem corpo")
    e.set_defaults(f=cmd_enriquecer)

    k = sub.add_parser("capas")
    k.add_argument("--limite", type=int, default=400)
    k.add_argument("--paralelo", type=int, default=8)
    k.set_defaults(f=cmd_capas)

    n = sub.add_parser("pendentes")
    n.add_argument("--limite", type=int, default=500)
    n.add_argument("--saida", default="dados/pendentes.json")
    n.set_defaults(f=cmd_pendentes)

    cl = sub.add_parser("classificar")
    cl.add_argument("--arquivo", required=True)
    cl.set_defaults(f=cmd_classificar)

    ac = sub.add_parser("autoclassificar")
    ac.add_argument("--limite", type=int, default=120)
    ac.add_argument("--paralelo", type=int, default=4)
    ac.add_argument("--modelo", default="claude-haiku-4-5-20251001",
                    help="ID COMPLETO e datado; alias muda a série histórica sem commit")
    ac.set_defaults(f=cmd_autoclassificar)

    cp = sub.add_parser("compactar")
    cp.add_argument("--dias-corpo", type=int, default=21)
    cp.add_argument("--dias-janela", type=int, default=120)
    cp.set_defaults(f=cmd_compactar)

    ex = sub.add_parser("exportar")
    ex.add_argument("--saida", default=ACERVO)
    ex.set_defaults(f=cmd_exportar)

    im = sub.add_parser("importar")
    im.add_argument("--arquivo", default=ACERVO)
    im.set_defaults(f=cmd_importar)

    sub.add_parser("reclusterizar").set_defaults(f=cmd_reclusterizar)
    sub.add_parser("reeixos").set_defaults(f=cmd_reeixos)
    sub.add_parser("reenriquecer").set_defaults(f=cmd_reenriquecer)
    sub.add_parser("publicar").set_defaults(f=cmd_publicar)
    sub.add_parser("estado").set_defaults(f=cmd_estado)

    args = p.parse_args()
    args.f(args)


if __name__ == "__main__":
    main()
