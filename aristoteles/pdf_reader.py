"""
pdf_reader.py — Extração de texto de PDFs com fallback progressivo.

Ordem de tentativa:
    1. pdfplumber  (mais fiel ao layout)
    2. PyPDF2      (mais simples, bom fallback)
    3. pytesseract (OCR para PDFs baseados em imagem)
"""

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Tempo máximo de espera (segundos) para o arquivo ficar disponível (ex: cópia ainda em andamento)
_LOCK_WAIT_S = 0.5
_LOCK_RETRIES = 6


def _aguardar_arquivo_pronto(caminho: Path) -> bool:
    """
    Tenta abrir o arquivo em modo binário algumas vezes antes de desistir.
    Útil no Windows onde o arquivo pode estar bloqueado logo após ser criado.
    """
    for _ in range(_LOCK_RETRIES):
        try:
            with open(caminho, "rb"):
                return True
        except (IOError, PermissionError):
            time.sleep(_LOCK_WAIT_S)
    return False


# ── Extratores individuais ────────────────────────────────────────────────────

def _ler_pdfplumber(caminho: Path) -> str:
    """Extrai texto com pdfplumber (preserva layout e espaços)."""
    try:
        import pdfplumber  # import tardio para não travar se não instalado
    except ImportError:
        logger.warning("pdfplumber não instalado, pulando.")
        return ""

    texto = ""
    try:
        with pdfplumber.open(caminho) as pdf:
            for pagina in pdf.pages:
                t = pagina.extract_text()
                if t:
                    texto += t + "\n"
    except Exception as exc:
        logger.warning("pdfplumber falhou para '%s': %s", caminho.name, exc)

    return texto.strip()


def _ler_pypdf2(caminho: Path) -> str:
    """Extrai texto com PyPDF2 (fallback simples)."""
    try:
        import PyPDF2  # noqa: N813
    except ImportError:
        logger.warning("PyPDF2 não instalado, pulando.")
        return ""

    texto = ""
    try:
        with open(caminho, "rb") as fh:
            reader = PyPDF2.PdfReader(fh)
            for pagina in reader.pages:
                t = pagina.extract_text()
                if t:
                    texto += t + "\n"
    except Exception as exc:
        logger.warning("PyPDF2 falhou para '%s': %s", caminho.name, exc)

    return texto.strip()


def _ler_ocr(caminho: Path) -> str:
    """
    Converte páginas do PDF em imagens e executa OCR (pytesseract).
    Requer: pytesseract + Tesseract-OCR instalado + pdf2image + poppler.
    """
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        logger.warning("pytesseract/pdf2image não instalados, OCR indisponível.")
        return ""

    texto = ""
    try:
        imagens = convert_from_path(str(caminho), dpi=300)
        for img in imagens:
            t = pytesseract.image_to_string(img, lang="por")
            if t:
                texto += t + "\n"
    except Exception as exc:
        logger.error("OCR falhou para '%s': %s", caminho.name, exc)

    return texto.strip()


# ── Função pública ────────────────────────────────────────────────────────────

def extrair_texto(caminho: Path) -> str:
    """
    Extrai e retorna o texto completo de um PDF.

    Tenta os extratores na seguinte ordem:
        1. pdfplumber → 2. PyPDF2 → 3. OCR

    Retorna string vazia se todos falharem.
    """
    if not caminho.exists():
        logger.error("Arquivo não encontrado: %s", caminho)
        return ""

    if not _aguardar_arquivo_pronto(caminho):
        logger.error("Arquivo bloqueado/inacessível após %d tentativas: %s",
                     _LOCK_RETRIES, caminho.name)
        return ""

    logger.info("Lendo PDF: %s", caminho.name)

    texto = _ler_pdfplumber(caminho)
    if texto:
        logger.debug("Texto extraído via pdfplumber (%d chars)", len(texto))
        return texto

    logger.info("pdfplumber retornou texto vazio, tentando PyPDF2…")
    texto = _ler_pypdf2(caminho)
    if texto:
        logger.debug("Texto extraído via PyPDF2 (%d chars)", len(texto))
        return texto

    logger.info("PyPDF2 retornou texto vazio, tentando OCR…")
    texto = _ler_ocr(caminho)
    if texto:
        logger.debug("Texto extraído via OCR (%d chars)", len(texto))
        return texto

    logger.error("Impossível extrair texto de '%s' (PDF pode estar corrompido ou protegido)",
                 caminho.name)
    return ""
