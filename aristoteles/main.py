"""
main.py — Ponto de entrada do sistema de processamento de notas fiscais.

Fluxo:
  1. Carrega configuracoes do Supabase (conf_aristoteles)
  2. Configura logging
  3. Processa PDFs ja existentes na pasta monitorada
  4. Inicia watchdog para novos arquivos
  5. Mantem loop principal ate CTRL+C
"""

import logging
import os
import platform
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from extractor import extrair_dados
from pdf_reader import extrair_texto
from supabase_client import SupabaseClient, carregar_conf_aristoteles
from utils import configurar_logging, criar_pastas
from watcher import iniciar_monitoramento

# ── Configuracoes de runtime (nao vem do banco) ───────────────────────────────

LOG_FILE    = os.getenv("LOG_FILE", "/app/logs/processamento.log")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

# ── Configuracoes de negocio (carregadas do Supabase em main()) ───────────────

PASTA_MONITORADA: Path = Path("/app/pdfs")
SMB_PATH:         str  = ""
SMB_USER:         str  = ""
SMB_PASSWORD:     str  = ""

logger = logging.getLogger(__name__)

# ── Client singleton ───────────────────────────────────────────────────────────

_supabase_client: SupabaseClient | None = None


def _obter_supabase_client() -> SupabaseClient:
    """Cria o SupabaseClient uma unica vez por execucao, carregando o cache."""
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = SupabaseClient()
        _supabase_client.carregar_numeros_existentes()
    return _supabase_client


# ── Extracao thread-safe (sem estado compartilhado) ───────────────────────────

def _extrair_pdf(caminho: Path) -> tuple[Path, dict | None]:
    """
    Extrai texto e dados de um PDF. Thread-safe.
    Retorna (caminho, dados) ou (caminho, None) em caso de falha.
    """
    try:
        texto = extrair_texto(caminho)
    except Exception as exc:
        logger.error("Erro ao ler PDF '%s': %s", caminho.name, exc)
        return caminho, None

    if not texto:
        logger.error("PDF vazio ou corrompido: '%s'", caminho.name)
        return caminho, None

    try:
        dados = extrair_dados(texto, caminho.name)
    except Exception as exc:
        logger.error("Erro na extracao de dados de '%s': %s", caminho.name, exc)
        return caminho, None

    if not dados:
        logger.error("Falha de extracao (campos obrigatorios ausentes): '%s'", caminho.name)
        return caminho, None

    return caminho, dados


# ── Processamento de um unico PDF (watcher real-time) ─────────────────────────

def processar_pdf(caminho: Path) -> None:
    """
    Processa um arquivo PDF recebido pelo watcher:
      1. Extrai texto e dados
      2. Verifica duplicidade via cache e insere no Supabase
      3. Move o arquivo para /processados ou /erro
    """
    pasta_proc, pasta_erro = criar_pastas(PASTA_MONITORADA)
    logger.info("Iniciando: %s", caminho.name)

    caminho, dados = _extrair_pdf(caminho)
    if dados is None:
        _mover(caminho, pasta_erro)
        return

    try:
        db = _obter_supabase_client()
        inserido = db.inserir_nota(dados)
    except Exception as exc:
        logger.error("Erro Supabase ao processar '%s': %s", caminho.name, exc)
        _mover(caminho, pasta_erro)
        return

    _mover(caminho, pasta_proc)

    if inserido:
        logger.info("Sucesso: '%s' | Nota: %s | Valor: %s",
                    caminho.name, dados["NumeroNota"], dados["ValorLiquido"])
    else:
        logger.info("Duplicidade: '%s' ja existe (nao inserido)", caminho.name)


def _mover(origem: Path, destino_dir: Path) -> None:
    """Move arquivo para destino_dir. Adiciona sufixo se ja existir."""
    destino = destino_dir / origem.name
    if destino.exists():
        destino = destino_dir / f"{origem.stem}_dup{int(time.time())}{origem.suffix}"
    try:
        shutil.move(str(origem), str(destino))
        logger.debug("Arquivo movido: %s -> %s", origem.name, destino_dir.name)
    except Exception as exc:
        logger.error("Nao foi possivel mover '%s' para '%s': %s",
                     origem.name, destino_dir, exc)


# ── Processamento de existentes ao iniciar ────────────────────────────────────

_PASTAS_IGNORADAS = {"processados", "erro"}


def processar_existentes(pasta: Path) -> None:
    """
    Ao iniciar, processa PDFs ja presentes na pasta monitorada em 3 etapas:
      1. Extracao paralela
      2. Insercao em lote no Supabase
      3. Movimentacao dos arquivos
    """
    pdfs = sorted(
        p for p in pasta.rglob("*.pdf")
        if not any(parte in _PASTAS_IGNORADAS for parte in p.parts)
    )
    if not pdfs:
        return

    logger.info(
        "Encontrado(s) %d PDF(s) existente(s) — extraindo em paralelo (workers=%d)...",
        len(pdfs), MAX_WORKERS,
    )
    pasta_proc, pasta_erro = criar_pastas(pasta)

    resultados: dict[Path, dict | None] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(_extrair_pdf, pdf): pdf for pdf in pdfs}
        for futuro in as_completed(futuros):
            caminho, dados = futuro.result()
            resultados[caminho] = dados

    validos: dict[Path, dict] = {c: d for c, d in resultados.items() if d is not None}
    invalidos: list[Path] = [c for c, d in resultados.items() if d is None]

    sucesso_caminhos: list[Path] = []
    if validos:
        try:
            db = _obter_supabase_client()
            db.inserir_notas_lote(list(validos.values()))
            sucesso_caminhos = list(validos.keys())
        except Exception as exc:
            logger.error("Erro Supabase no lote: %s", exc)
            invalidos.extend(validos.keys())

    for caminho in sucesso_caminhos:
        _mover(caminho, pasta_proc)
    for caminho in invalidos:
        _mover(caminho, pasta_erro)


# ── Main ──────────────────────────────────────────────────────────────────────

def _montar_rede() -> None:
    """Monta o compartilhamento de rede (Linux: mount.cifs | Windows: net use)."""
    if not SMB_USER:
        return

    if platform.system() == "Linux":
        if not SMB_PATH:
            logger.warning("SMB_PATH nao definido — montagem ignorada.")
            return
        mount_str = str(PASTA_MONITORADA)
        result = subprocess.run(["mountpoint", "-q", mount_str], capture_output=True)
        if result.returncode == 0:
            logger.info("Compartilhamento ja montado em: %s", mount_str)
            return
        subprocess.run(["sudo", "mkdir", "-p", mount_str], capture_output=True)
        try:
            subprocess.run(
                ["sudo", "mount", "-t", "cifs", SMB_PATH, mount_str,
                 "-o", f"username={SMB_USER},password={SMB_PASSWORD},iocharset=utf8,vers=3.0,uid=1000,gid=1000"],
                check=True, capture_output=True, text=True,
            )
            logger.info("Compartilhamento montado em: %s", mount_str)
        except subprocess.CalledProcessError as exc:
            logger.warning("mount.cifs retornou erro: %s", exc.stderr.strip())
    else:
        caminho_str = str(PASTA_MONITORADA)
        if not caminho_str.startswith("\\\\"):
            return
        partes = caminho_str.lstrip("\\").split("\\")
        unc_share = f"\\\\{partes[0]}\\{partes[1]}" if len(partes) >= 2 else caminho_str
        try:
            subprocess.run(
                ["net", "use", unc_share, f"/user:{SMB_USER}", SMB_PASSWORD],
                check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as exc:
            logger.warning("net use retornou erro: %s", exc.stderr.strip())


def main() -> None:
    global PASTA_MONITORADA, SMB_PATH, SMB_USER, SMB_PASSWORD

    configurar_logging(LOG_FILE)
    logger.info("=" * 60)
    logger.info("Sistema de Notas Fiscais iniciado")

    # Carrega configuracoes do Supabase
    try:
        conf = carregar_conf_aristoteles()
        PASTA_MONITORADA = Path(conf["pasta_monitorada"])
        SMB_PATH         = conf["smb_path"]
        SMB_USER         = conf["smb_user"]
        SMB_PASSWORD     = conf["smb_password"]
        logger.info("Configuracoes carregadas do Supabase")
    except Exception as exc:
        logger.error("Erro ao carregar conf_aristoteles: %s — usando defaults", exc)

    logger.info("Pasta monitorada: %s", PASTA_MONITORADA)
    logger.info("=" * 60)

    _montar_rede()
    PASTA_MONITORADA.mkdir(parents=True, exist_ok=True)
    processar_existentes(PASTA_MONITORADA)

    observer = iniciar_monitoramento(PASTA_MONITORADA, processar_pdf)

    logger.info("Aguardando novos PDFs... (CTRL+C para encerrar)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Encerrando monitoramento...")
    finally:
        observer.stop()
        observer.join()
        logger.info("Sistema encerrado.")


if __name__ == "__main__":
    main()
