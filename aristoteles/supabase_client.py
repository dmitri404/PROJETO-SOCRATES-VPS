"""
supabase_client.py — Integracao com PostgreSQL/Supabase.

Substitui sheets.py: le configuracoes de conf_aristoteles e
salva notas fiscais na tabela faturamento.
"""

import logging
import os
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)


def _conectar():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST", "supabase-db"),
        port=int(os.getenv("SUPABASE_DB_PORT", "5432")),
        dbname=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER", "postgres"),
        password=os.getenv("SUPABASE_DB_PASSWORD"),
        connect_timeout=10,
    )


def carregar_conf_aristoteles() -> dict:
    """Le a linha ativa de conf_aristoteles e retorna como dict."""
    conn = _conectar()
    cur = conn.cursor()
    cur.execute(
        "SELECT smb_path, smb_user, smb_password, pasta_monitorada "
        "FROM conf_aristoteles WHERE ativo = TRUE LIMIT 1"
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise ValueError("Nenhuma configuracao ativa em conf_aristoteles")
    return {
        "smb_path":         row[0] or "",
        "smb_user":         row[1] or "",
        "smb_password":     row[2] or "",
        "pasta_monitorada": row[3],
    }


class SupabaseClient:
    """
    Substitui GoogleSheetsClient com a mesma interface publica.
    Salva notas na tabela faturamento com deduplicacao por numero_nota.
    """

    def __init__(self) -> None:
        self._numeros_cache: Optional[set[str]] = None

    def carregar_numeros_existentes(self) -> None:
        """Carrega todos os numero_nota existentes em cache."""
        try:
            conn = _conectar()
            cur = conn.cursor()
            cur.execute("SELECT numero_nota FROM faturamento")
            self._numeros_cache = {row[0] for row in cur.fetchall()}
            cur.close()
            conn.close()
            logger.info("Cache de numeros carregado: %d registro(s).", len(self._numeros_cache))
        except Exception as exc:
            logger.error("Erro ao carregar cache: %s", exc)
            self._numeros_cache = set()

    def numero_ja_existe(self, numero_nota: str) -> bool:
        if self._numeros_cache is not None:
            return numero_nota in self._numeros_cache
        try:
            conn = _conectar()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM faturamento WHERE numero_nota = %s LIMIT 1", (numero_nota,))
            existe = cur.fetchone() is not None
            cur.close()
            conn.close()
            return existe
        except Exception as exc:
            logger.error("Erro ao verificar duplicata: %s", exc)
            return False

    def inserir_nota(self, dados: dict) -> bool:
        """Insere uma nota. Retorna True se inserida, False se duplicata."""
        numero = dados.get("NumeroNota", "")
        if self.numero_ja_existe(numero):
            logger.info("Duplicidade: Nota '%s' ja existe.", numero)
            return False
        try:
            conn = _conectar()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO faturamento (numero_nota, data_emissao, cnpj_emitente, cnpj_tomador, "
                "nome_tomador, valor_total, valor_liquido, arquivo) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (numero_nota) DO NOTHING",
                (numero, dados.get("DataEmissao"), dados.get("CNPJEmitente"),
                 dados.get("CNPJTomador"), dados.get("NomeTomador"),
                 dados.get("ValorTotal"), dados.get("ValorLiquido"), dados.get("Arquivo"))
            )
            conn.commit()
            cur.close()
            conn.close()
            if self._numeros_cache is not None:
                self._numeros_cache.add(numero)
            logger.info("Nota '%s' inserida com sucesso.", numero)
            return True
        except Exception as exc:
            logger.error("Erro ao inserir nota '%s': %s", numero, exc)
            raise

    def inserir_notas_lote(self, lista_dados: list[dict]) -> tuple[int, int]:
        """Insere multiplas notas, filtrando duplicatas. Retorna (inseridos, duplicatas)."""
        if not lista_dados:
            return 0, 0

        novas = []
        duplicatas = 0
        for dados in lista_dados:
            numero = dados.get("NumeroNota", "")
            if self.numero_ja_existe(numero):
                duplicatas += 1
            else:
                novas.append(dados)

        if not novas:
            logger.info("Lote: 0 inserido(s), %d duplicata(s).", duplicatas)
            return 0, duplicatas

        try:
            conn = _conectar()
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO faturamento (numero_nota, data_emissao, cnpj_emitente, cnpj_tomador, "
                "nome_tomador, valor_total, valor_liquido, arquivo) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (numero_nota) DO NOTHING",
                [(d.get("NumeroNota"), d.get("DataEmissao"), d.get("CNPJEmitente"),
                  d.get("CNPJTomador"), d.get("NomeTomador"),
                  d.get("ValorTotal"), d.get("ValorLiquido"), d.get("Arquivo"))
                 for d in novas]
            )
            conn.commit()
            cur.close()
            conn.close()
            if self._numeros_cache is not None:
                self._numeros_cache.update(d.get("NumeroNota", "") for d in novas)
            inseridos = len(novas)
            logger.info("Lote: %d inserido(s), %d duplicata(s).", inseridos, duplicatas)
            return inseridos, duplicatas
        except Exception as exc:
            logger.error("Erro ao inserir lote: %s", exc)
            raise
