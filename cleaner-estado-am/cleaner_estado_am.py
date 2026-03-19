#!/usr/bin/env python3
"""
cleaner_estado_am.py
Processa portal_estado_am.pagamentos, extrai campos de descricao_ob
e insere em portal_estado_am.pagamentos_treated.
"""

import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip3 install psycopg2-binary")
    sys.exit(1)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cleaner_estado_am_config.json")

# ── Prefixos NF (reutilizados do cleaner principal) ────────────────────────────
_NF_PREFIXES = sorted([
    "Nota Fiscal de Serviço Nº ",
    "Nota Fiscal de Serviço n° ",
    "Nota Fiscal de Serviço nº ",
    "NOTA FISCAL N° ",
    "Nota Fiscal: ",
    "Eletrônica Nº ",
    "Serviço Nº",
    "Serviço nº ",
    "DE SERVIÇO ",
    "NFS-E Nº ",
    "NFS-E N° ",
    "NFS-e N° ",
    "NFS-e n.º ",
    "NFS-e nº ",
    "NFS-e n° ",
    "NFS-E: 0",
    "NFS-E: ",
    "NFS-E:",
    "NFS-e: ",
    "NFS-e:",
    "NFS: ",
    "NFSE Nº ",
    "NF-e n°",
    "NF Nº ",
    "NF: ",
    "NF n. ",
    "NFS n. ",
    "PGTO DA NFS-e ",
    "NFS-e ",
    "NFS-E ",
    "NF ",
    "fatura n° ",
    "fatura n°",
    "FATURA Nº ",
    "FATURA: ",
    "Fatura de nº ",
], key=len, reverse=True)

_NF_PATTERN = re.compile(
    r'(?:' + '|'.join(re.escape(p) for p in _NF_PREFIXES) + r')(\d+)',
    re.IGNORECASE,
)

# ── Extração de campos ─────────────────────────────────────────────────────────

def parse_descricao_ob(value):
    result = {
        "nl_numero": None,
        "nf_numero": None,
        "nf_data":   None,
        "mes_ref":   None,
        "processo":  None,
        "contrato":  None,
    }
    if not value:
        return result

    # NL: 2025NL0001591
    m = re.search(r'(\d{4}NL\d{4,7})', value)
    if m:
        result["nl_numero"] = m.group(1)

    # NF número
    m = _NF_PATTERN.search(value)
    if m:
        result["nf_numero"] = m.group(1)

    # Data NF: "DE 14/2/2025", "de 06/01/26"
    m = re.search(r'\bDE\s+(\d{1,2}/\d{1,2}/\d{2,4})', value, re.IGNORECASE)
    if not m:
        m = re.search(r'\b((?:0[1-9]|[12]\d|3[01])/(?:0[1-9]|1[0-2])/\d{2,4})\b', value)
    if m:
        result["nf_data"] = m.group(1)

    # Mês ref: "JAN/2025", "SET/2025" ou entre " - " separadores
    m = re.search(
        r'\b(JAN|FEV|MAR|ABR|MAI|JUN|JUL|AGO|SET|OUT|NOV|DEZ)/(\d{4})\b',
        value, re.IGNORECASE
    )
    if m:
        result["mes_ref"] = f"{m.group(1).upper()}/{m.group(2)}"
    else:
        # Fallback: mês entre parênteses ex. (NOV/2025)
        m = re.search(
            r'\((JAN|FEV|MAR|ABR|MAI|JUN|JUL|AGO|SET|OUT|NOV|DEZ)/(\d{4})\)',
            value, re.IGNORECASE
        )
        if m:
            result["mes_ref"] = f"{m.group(1).upper()}/{m.group(2)}"

    # Processo: "PROC. 028101.009412/2025-05" ou "Processo: 043102.000011/2025-73"
    m = re.search(r'(?:PROC\.?|Processo)\s*:?\s*([\d\.]+/\d{2,4}-\d+)', value, re.IGNORECASE)
    if m:
        result["processo"] = m.group(1)

    # Contrato: "CT. 43/2021", "CT 011/2022", "CONTRATO 70/2023"
    m = re.search(r'(?:CONTRATO|CT\.?)\s*:?\s*(\d+/\d{4})', value, re.IGNORECASE)
    if m:
        result["contrato"] = m.group(1)

    return result


# ── DB ────────────────────────────────────────────────────────────────────────

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def setup_logging(config):
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    logger = logging.getLogger("cleaner_estado_am")
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

def get_connection(config):
    db = config["database"]
    return psycopg2.connect(
        host=db["host"], port=db["port"], dbname=db["name"],
        user=db["user"], password=db["password"],
        options="-c search_path=portal_estado_am",
    )

def fetch_untreated(conn, batch_size):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM pagamentos WHERE treatment IS NULL ORDER BY id LIMIT %s",
            (batch_size,)
        )
        return cur.fetchall()

def insert_treated(conn, row, parsed):
    cols = ["id", "exercicio", "mes", "orgao", "credor", "data", "num_ob",
            "num_nl", "num_ne", "valor_ob", "credor_ob", "descricao_ob",
            "nl_numero", "nf_numero", "nf_data", "mes_ref", "processo", "contrato"]
    values = [
        row["id"], row["exercicio"], row["mes"], row["orgao"], row["credor"],
        row["data"], row["num_ob"], row["num_nl"], row["num_ne"],
        row["valor_ob"], row["credor_ob"], row["descricao_ob"],
        parsed["nl_numero"], parsed["nf_numero"], parsed["nf_data"],
        parsed["mes_ref"], parsed["processo"], parsed["contrato"],
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO pagamentos_treated ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT (id) DO NOTHING",
            values
        )

def update_treatment(conn, row_id, status):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pagamentos SET treatment = %s, treatment_time = %s WHERE id = %s",
            (status, datetime.now(timezone.utc), row_id)
        )

def process_batch(conn, config, logger):
    batch_size = config["processing"]["batch_size"]
    rows = fetch_untreated(conn, batch_size)
    if not rows:
        return 0

    batch_id = str(uuid.uuid4())[:8]
    batch_start = time.time()
    ok = fail = 0

    logger.info(f"Batch {batch_id}: {len(rows)} registros")

    for row in rows:
        try:
            parsed = parse_descricao_ob(row["descricao_ob"])
            insert_treated(conn, row, parsed)
            update_treatment(conn, row["id"], "success")
            conn.commit()
            ok += 1
        except Exception as e:
            conn.rollback()
            logger.error(f"Row {row['id']}: {e}")
            try:
                update_treatment(conn, row["id"], "failure")
                conn.commit()
            except Exception:
                conn.rollback()
            fail += 1

    duration = int((time.time() - batch_start) * 1000)
    logger.info(f"Batch {batch_id}: {ok} ok, {fail} fail, {duration}ms")
    return len(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    logger = setup_logging(config)
    logger.info("Cleaner Estado AM iniciado")

    try:
        conn = get_connection(config)
        count = process_batch(conn, config, logger)
        if count == 0:
            logger.info("Nenhum registro pendente.")
        conn.close()
    except Exception as e:
        logger.error(f"Erro: {e}")
        sys.exit(1)

    logger.info("Cleaner Estado AM finalizado")

if __name__ == "__main__":
    main()
