"""
watcher.py — Monitoramento da pasta de entrada via watchdog.

Detecta novos arquivos PDF e aciona o processador configurado.
Monitora recursivamente todas as subpastas, exceto processados/ e erro/.
"""

import logging
from pathlib import Path
from typing import Callable

from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

logger = logging.getLogger(__name__)

# Subpastas gerenciadas pelo sistema — eventos nelas são ignorados
_PASTAS_IGNORADAS = {"processados", "erro"}


class PDFHandler(FileSystemEventHandler):
    """
    Handler watchdog que filtra eventos de criação de PDFs
    e aciona a função de processamento recebida como parâmetro.
    """

    def __init__(self, processor: Callable[[Path], None]) -> None:
        super().__init__()
        self._processor = processor
        # Rastreia arquivos em processamento para evitar disparos duplos
        self._em_processamento: set[str] = set()

    def on_created(self, event: FileCreatedEvent) -> None:
        """Chamado pelo watchdog quando um arquivo é criado na pasta monitorada."""
        if event.is_directory:
            return

        caminho = Path(str(event.src_path))

        # Ignora arquivos que não sejam PDF
        if caminho.suffix.lower() != ".pdf":
            return

        # Ignora PDFs dentro das subpastas gerenciadas (processados/, erro/)
        # Verifica todos os segmentos do caminho, não só o pai imediato
        if any(parte in _PASTAS_IGNORADAS for parte in caminho.parts):
            return

        # Chave única inclui o caminho relativo para evitar colisões entre subpastas
        nome = str(caminho)

        # Proteção contra disparos duplos (alguns sistemas geram 2 eventos)
        if nome in self._em_processamento:
            logger.debug("Evento duplicado ignorado para: %s", nome)
            return

        self._em_processamento.add(nome)
        logger.info("Novo PDF detectado: %s", caminho.name)

        try:
            self._processor(caminho)
        except Exception as exc:
            # O processador tem seu próprio tratamento de erros;
            # capturamos aqui apenas para garantir que o conjunto seja limpo.
            logger.error("Erro não tratado ao processar '%s': %s", caminho.name, exc)
        finally:
            self._em_processamento.discard(nome)


def iniciar_monitoramento(
    pasta: Path,
    processor: Callable[[Path], None],
) -> Observer:
    """
    Cria e inicia um Observer watchdog para *pasta*.

    Parâmetros:
        pasta:     diretório raiz a ser monitorado (recursivo).
        processor: função chamada com Path do PDF detectado.

    Retorna o Observer em execução (chamar .stop() + .join() para encerrar).
    """
    handler = PDFHandler(processor)
    observer = Observer()
    observer.schedule(handler, str(pasta), recursive=True)
    observer.start()
    logger.info("Monitoramento iniciado em: %s", pasta.resolve())
    return observer
