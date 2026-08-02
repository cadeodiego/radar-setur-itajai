"""Miniaturas do wall.

Duas regras que vêm de fora do código:

1. Imagem hostil é vetor de exaustão de memória. Recusa acima de 8 MB, acima de
   8000 px em qualquer lado, ou fora de jpeg/png/webp/gif — antes de decodificar.
2. Quem não tem capa NÃO ganha placeholder cinza. Ganha cartão tipográfico
   montado pelo próprio wall em CSS (ver web/), que responde ao tema e parece
   escolha editorial. Por isso aqui não se gera imagem de fallback: a ausência
   é devolvida como ausência, e a página decide o que fazer com ela.
"""
from __future__ import annotations

import base64
import hashlib
import io
from pathlib import Path

from PIL import Image, ImageOps

from . import coleta

MAX_BYTES = 8 * 1024 * 1024
MAX_LADO = 8000
# Piso de foto de notícia. Abaixo disto é logo, selo ou favicon — e a extração
# de og:image cai nesses com frequência quando o portal não marca a foto da
# matéria. Um logo esticado em 16:9 é pior que o cartão tipográfico, que ao
# menos é uma escolha.
MIN_LARGURA, MIN_ALTURA = 320, 180
LARGURA, ALTURA = 480, 270      # 16:9
QUALIDADE = 72
LQIP_LARGURA = 24

MIMES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "image/gif", "image/avif"}


def caminho_de(raiz: Path, h: str, formato: str = "webp") -> Path:
    """Fanout de 2 níveis: nenhum diretório passa de ~4 mil arquivos."""
    return raiz / h[:2] / h[2:4] / f"{h}.{formato}"


def processar(url: str, raiz: Path) -> dict | None:
    """Baixa, corta em 16:9, grava e devolve os metadados de rd_capa."""
    status, corpo = coleta.buscar(url, ua=coleta.UA_ARTIGO, timeout=25)
    if status != 200 or not corpo:
        return {"_erro": f"http {status}"}
    if len(corpo) > MAX_BYTES:
        return {"_erro": f"grande demais ({len(corpo) // 1024} KB)"}

    try:
        img = Image.open(io.BytesIO(corpo))
        img.load()
    except Exception as e:
        return {"_erro": f"não é imagem legível ({type(e).__name__})"}

    if max(img.size) > MAX_LADO:
        return {"_erro": f"dimensão suspeita {img.size}"}
    if img.size[0] < MIN_LARGURA or img.size[1] < MIN_ALTURA:
        return {"_erro": f"pequena demais {img.size} — provável logo ou ícone"}

    img = ImageOps.exif_transpose(img)
    if img.mode not in ("RGB", "L"):
        fundo = Image.new("RGB", img.size, (255, 255, 255))
        img = img.convert("RGBA")
        fundo.paste(img, mask=img.split()[-1])
        img = fundo
    else:
        img = img.convert("RGB")

    # corte central 16:9 — o assunto da foto de notícia mora no meio
    capa = ImageOps.fit(img, (LARGURA, ALTURA), method=Image.LANCZOS, centering=(0.5, 0.42))

    h = hashlib.sha1(corpo).hexdigest()
    destino = caminho_de(raiz, h)
    destino.parent.mkdir(parents=True, exist_ok=True)
    capa.save(destino, "WEBP", quality=QUALIDADE, method=5)

    # LQIP: o wall pinta isto borrado antes de a capa real chegar
    pequena = capa.resize((LQIP_LARGURA, max(1, LQIP_LARGURA * ALTURA // LARGURA)), Image.LANCZOS)
    buf = io.BytesIO()
    pequena.save(buf, "WEBP", quality=40, method=4)
    lqip = "data:image/webp;base64," + base64.b64encode(buf.getvalue()).decode()

    return {
        "hash": h,
        "largura": LARGURA,
        "altura": ALTURA,
        "bytes": destino.stat().st_size,
        "mime": "image/webp",
        "origem_url": url,
        "lqip": lqip if len(lqip) < 600 else None,
        "formato": "webp",
    }
