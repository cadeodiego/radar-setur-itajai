"""Classificação automática de humor pela API da Anthropic.

Só entra em cena na operação diária. As 117 avaliações da Fase 1 foram leitura
humana e continuam gravadas como tal (`origem='humano'`) — este módulo não
reescreve nada, só acrescenta.

Duas travas que fazem a nota ser defensável e não opinião de caixa-preta:

1. **Modelo datado, nunca alias.** `claude-haiku-4-5-20251001`, não "latest".
   Alias troca o comportamento por baixo e a série histórica muda sem commit.
2. **Evidência conferida por busca literal.** O modelo é obrigado a citar
   trechos do texto avaliado; o que não existir no texto derruba a confiança
   para baixa e manda o item para revisão humana. Alucinação vira sinal, não
   número bonito.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

API = "https://api.anthropic.com/v1/messages"
MODELO_PADRAO = "claude-haiku-4-5-20251001"
VERSAO_API = "2023-06-01"

# preço por milhão de tokens, para o custo ir gravado junto com a avaliação
PRECO = {"claude-haiku-4-5-20251001": (1.00, 5.00)}

FERRAMENTA = {
    "name": "registrar_avaliacao",
    "description": "Registra a avaliação de favorabilidade da matéria.",
    "input_schema": {
        "type": "object",
        "properties": {
            "nota": {"type": "integer", "minimum": 0, "maximum": 10,
                     "description": "Nota de favorabilidade pela rubrica."},
            "justificativa": {"type": "string",
                              "description": "UMA frase explicando a nota."},
            "evidencias": {
                "type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3,
                "description": ("Trechos COPIADOS LITERALMENTE do texto fornecido, "
                                "palavra por palavra. Nunca parafrasear, nunca inventar."),
            },
            "confianca": {"type": "string", "enum": ["alta", "media", "baixa"]},
        },
        "required": ["nota", "justificativa", "evidencias", "confianca"],
    },
}


class SemChave(RuntimeError):
    pass


def _chave() -> str:
    k = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not k:
        raise SemChave(
            "ANTHROPIC_API_KEY ausente. No GitHub Actions ela vem do secret de mesmo nome; "
            "localmente, exporte antes de rodar. Sem chave o comando não inventa nota."
        )
    return k


def avaliar(item: dict, rubrica_texto: str, prompt_tpl: str, cliente: str,
            modelo: str = MODELO_PADRAO, timeout: int = 90) -> dict:
    """Devolve a avaliação bruta do modelo, ou {'_erro': ...}."""
    corpo_txt = (item.get("corpo") or "")[:6000]
    conteudo = prompt_tpl.format(
        cliente=cliente, rubrica=rubrica_texto,
        titulo=item["titulo"], veiculo=item.get("veiculo") or "desconhecido",
        texto=(item.get("resumo") or "") + "\n\n" + corpo_txt,
    )
    pedido = {
        "model": modelo,
        "max_tokens": 700,
        "temperature": 0,
        "tools": [FERRAMENTA],
        "tool_choice": {"type": "tool", "name": "registrar_avaliacao"},
        "messages": [{"role": "user", "content": conteudo}],
    }
    req = urllib.request.Request(
        API, data=json.dumps(pedido).encode("utf-8"),
        headers={
            "x-api-key": _chave(),
            "anthropic-version": VERSAO_API,
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            resposta = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detalhe = e.read().decode("utf-8", "replace")[:300]
        return {"_erro": f"HTTP {e.code}: {detalhe}"}
    except Exception as e:
        return {"_erro": f"{type(e).__name__}: {e}"}

    bloco = next((b for b in resposta.get("content", []) if b.get("type") == "tool_use"), None)
    if not bloco:
        return {"_erro": "resposta sem tool_use"}

    uso = resposta.get("usage", {})
    entrada, saida = uso.get("input_tokens", 0), uso.get("output_tokens", 0)
    p_in, p_out = PRECO.get(modelo, (0.0, 0.0))
    return {
        **bloco["input"],
        "_modelo": modelo,
        "_tokens_in": entrada,
        "_tokens_out": saida,
        "_custo_usd": round(entrada / 1e6 * p_in + saida / 1e6 * p_out, 6),
    }
