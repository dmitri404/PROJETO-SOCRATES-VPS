"""
utils.py — Funções auxiliares compartilhadas entre os módulos.
"""

import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── CNPJ ─────────────────────────────────────────────────────────────────────

# Captura CNPJs formatados (XX.XXX.XXX/XXXX-XX) e não formatados (14 dígitos)
_CNPJ_RE = re.compile(
    r'\b(\d{2}[.\-\s]?\d{3}[.\-\s]?\d{3}[/\s]?\d{4}[-\s]?\d{2})\b'
)


def formatar_cnpj(cnpj: str) -> str:
    """Normaliza qualquer string de CNPJ para XX.XXX.XXX/XXXX-XX."""
    digits = re.sub(r'\D', '', cnpj)
    if len(digits) == 14:
        return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
    return cnpj


def extrair_cnpjs(texto: str) -> list[str]:
    """
    Extrai CNPJs únicos do texto na ordem de aparecimento.
    Convenção usada pelo sistema:
        [0] → CNPJ do Emitente
        [1] → CNPJ do Tomador
    """
    matches = _CNPJ_RE.findall(texto)
    vistos: list[str] = []
    for m in matches:
        fmt = formatar_cnpj(m)
        if fmt not in vistos:
            vistos.append(fmt)
    return vistos


# ── Valores monetários ────────────────────────────────────────────────────────

def limpar_valor(valor_str: str) -> Optional[float]:
    """
    Converte string monetária brasileira para float.
    Exemplos:
        '1.234,56'   → 1234.56
        'R$ 500,00'  → 500.0
        '1234.56'    → 1234.56  (formato ponto como decimal)
    Retorna None se a conversão falhar.
    """
    if not valor_str:
        return None

    # Remove símbolo de moeda e espaços
    v = re.sub(r'[R$\s]', '', valor_str.strip())

    # Formato BR: ponto = separador de milhar, vírgula = decimal
    if ',' in v and '.' in v:
        v = v.replace('.', '').replace(',', '.')
    elif ',' in v:
        # Apenas vírgula: assume decimal BR
        v = v.replace(',', '.')
    # else: apenas ponto — assume decimal anglo-saxão, não modifica

    try:
        return float(v)
    except ValueError:
        logger.warning("Não foi possível converter valor monetário: '%s'", valor_str)
        return None


def formatar_valor_br(valor: Optional[float]) -> str:
    """Formata float como string no padrão '1.234,56'. Retorna '' para None."""
    if valor is None:
        return ""
    return f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


# ── Pastas ────────────────────────────────────────────────────────────────────

def criar_pastas(base: Path) -> tuple[Path, Path]:
    """
    Cria e retorna as pastas 'processados' e 'erro' dentro de *base*.
    Não levanta exceção se já existirem.
    """
    processados = base / "processados"
    erro = base / "erro"
    processados.mkdir(parents=True, exist_ok=True)
    erro.mkdir(parents=True, exist_ok=True)
    return processados, erro


# ── Logging ───────────────────────────────────────────────────────────────────

def configurar_logging(log_file: str = "processamento.log") -> None:
    """
    Configura handlers de logging para arquivo e console.
    Deve ser chamado UMA ÚNICA VEZ na inicialização do main.py.
    """
    fmt = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=date_fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
