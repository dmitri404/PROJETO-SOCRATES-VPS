#!/usr/bin/env python3
"""
Cleaner Agent — Socrates Project
Job 1: Processes raw pagamentos data, cleans columns, inserts into pagamentos_treated.
Job 2: Parses SASI "Avaliação Diária" messages into mn_daily_evaluations.
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

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cleaner_config.json")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def setup_logging(config):
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = log_cfg.get("file", "cleaner.log")

    logger = logging.getLogger("cleaner")
    logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def get_connection(config):
    db = config["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
        options=f"-c search_path={db['schema']}"
    )

# ============================================================
# Job 1: Pagamentos Cleaning
# ============================================================

def strip_prefix(value, prefix):
    if value and value.startswith(prefix):
        return value[len(prefix):].strip()
    return value

def extract_valor(value):
    valor = None
    valor_anulado = None
    if not value:
        return valor, valor_anulado
    m_valor = re.search(r'Valor do pagamento:\s*([0-9.,]+)', value)
    if m_valor:
        valor = m_valor.group(1).strip().rstrip(',')
    m_anulado = re.search(r'Valor anulado do pagamento:\s*([0-9.,]+)', value)
    if m_anulado:
        valor_anulado = m_anulado.group(1).strip().rstrip(',')
    return valor, valor_anulado

def parse_descricao(value):
    result = {
        "nl_numero": None, "nf_numero": None, "nf_data": None,
        "mes_ref": None, "credor": None, "tipo_retencao": None,
    }
    if not value:
        return result

    m = re.search(r'NL\s*(?:n[ºo]|:)\s*(\d{4}NL\d{4,5})', value)
    if m:
        result["nl_numero"] = m.group(1)

    m = re.search(r'(?:NF(?:S-?[eE]|SE)?|[Nn]ota\s+[Ff]is[cx]al(?:\s+de\s+Servi[çc]o)?)\s*(?::?\s*[Nn]?[ºo°\.]*\s*)(\d+)', value)
    if m:
        result["nf_numero"] = m.group(1)

    m = re.search(r'\((\d{2}/\d{2}/\d{2,4})\)', value)
    if m:
        result["nf_data"] = m.group(1)

    m = re.search(r'\[MES(\d{2}/\d{2,4})\]', value)
    if m:
        result["mes_ref"] = m.group(1)
    else:
        m = re.search(r'PER[ÍI]ODO[:\s]+(?:DE\s+)?(.+?)(?:\s*[-–.](?:\s|$)|\s*(?:CONTRATO|PARCELA|PUD|T\.C|T\.A)|$)', value, re.IGNORECASE)
        if m:
            result["mes_ref"] = m.group(1).strip().rstrip(',.- ')[:20]

    upper = value.upper()
    if 'FUMIPEQ' in upper:
        result["tipo_retencao"] = "FUMIPEQ"
    elif re.search(r'RETEN[ÇC][ÃA]O\s+(?:DE\s+)?I\.?R\.?(?:\s|$|R\.?F)', upper) or 'RET IR ' in upper or 'RET. IR' in upper or ' IRRF' in upper:
        result["tipo_retencao"] = "IR"
    elif 'ISSQN' in upper or re.search(r'RETEN[ÇC][ÃA]O.*ISS\b', upper) or 'RET ISS' in upper or 'RET. ISS' in upper or 'RET-ISS' in upper:
        result["tipo_retencao"] = "ISS"
    elif re.search(r'\bINSS\b', upper):
        result["tipo_retencao"] = "INSS"
    elif re.search(r'\bFSS\b', upper) or 'RET FSS' in upper or 'RET-FSS' in upper or 'RET. FSS' in upper:
        result["tipo_retencao"] = "FSS"
    elif 'LIQUIDO' in upper or 'LÍQUIDO' in upper:
        result["tipo_retencao"] = "LIQUIDO"
    elif 'PARTE' in upper:
        result["tipo_retencao"] = "PARTE"

    if 'IIN' in upper or 'INN TECNOL' in upper:
        result["credor"] = "IIN"
    elif 'SASI' in upper:
        result["credor"] = "SASI"
    elif 'MDC' in upper:
        result["credor"] = "MDC"
    elif 'OZONIO' in upper:
        result["credor"] = "OZONIO"
    elif 'XMARKET' in upper or 'X MARKET' in upper:
        result["credor"] = "XMARKET"
    elif 'L S ' in upper or 'L.S' in upper or 'LS INFOR' in upper or 'LS LTDA' in upper or 'INFORMATICA E TELECOM' in upper:
        result["credor"] = "LS"
    else:
        if 'CENTRO DE COMANDO' in upper:
            result["credor"] = "IIN"
        elif 'PORTARIA' in upper:
            result["credor"] = "LS"
        elif 'ALERTA EMERGENCIAL' in upper or 'ALERTA ELETR' in upper or re.search(r'BOT.O DE P.NICO', upper):
            result["credor"] = "SASI"
        elif 'CONTEINERES' in upper or 'ARMAZENAMENTO DE ITENS' in upper or re.search(r'ARM.RIO COFRE', upper):
            result["credor"] = "MDC"
        elif 'LINK DE COMUNICA' in upper or 'LINK DE DADOS' in upper:
            result["credor"] = "OZONIO"
        elif 'PONTO ELETR' in upper:
            result["credor"] = "XMARKET"

    return result


def treat_row(row, config):
    treated = {}
    columns_treat = config["columns_treat"]

    for col in config["columns_copy"]:
        treated[col] = row.get(col)

    for col, rule_cfg in columns_treat.items():
        raw_value = row.get(col)
        rule = rule_cfg["rule"]

        if rule == "strip_prefix":
            treated[col] = strip_prefix(raw_value, rule_cfg["prefix"])
        elif rule == "extract_valor":
            valor, valor_anulado = extract_valor(raw_value)
            treated["valor"] = valor
            treated["valor_anulado"] = valor_anulado
        elif rule == "parse_descricao":
            treated[col] = strip_prefix(raw_value, rule_cfg.get("prefix", ""))
            parsed = parse_descricao(raw_value)
            treated.update(parsed)
        else:
            treated[col] = raw_value

    return treated


def fetch_untreated(conn, config):
    source = config["tables"]["source"]
    batch_size = config["processing"]["batch_size"]
    skip = config["processing"]["skip_already_treated"]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if skip:
            cur.execute(
                f"SELECT * FROM {source} WHERE treatment IS NULL ORDER BY created_at LIMIT %s",
                (batch_size,)
            )
        else:
            cur.execute(
                f"SELECT * FROM {source} ORDER BY created_at LIMIT %s",
                (batch_size,)
            )
        return cur.fetchall()


def insert_treated(conn, treated, config):
    target = config["tables"]["target"]
    columns = list(treated.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {target} ({col_names}) VALUES ({placeholders})",
            [treated[c] for c in columns]
        )


def update_treatment_status(conn, row_id, status, config):
    source = config["tables"]["source"]
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {source} SET treatment = %s, treatment_time = %s WHERE id = %s",
            (status, now, row_id)
        )


def log_to_db(conn, action, config, source_row_id=None, status=None, message=None,
              rows_processed=None, duration_ms=None, batch_id=None):
    log_table = config["tables"]["log"]

    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {log_table}
                (action, source_row_id, status, message, rows_processed, duration_ms, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (action, source_row_id, status, message, rows_processed, duration_ms, batch_id)
        )


def process_pagamentos_batch(conn, config, logger):
    rows = fetch_untreated(conn, config)

    if not rows:
        return 0

    batch_id = str(uuid.uuid4())
    batch_start = time.time()
    success_count = 0
    failure_count = 0

    logger.info(f"[pagamentos] Batch {batch_id[:8]}: Processing {len(rows)} rows")
    log_to_db(conn, "batch_start", config, batch_id=batch_id,
              message=f"Processing {len(rows)} rows")

    for row in rows:
        row_id = row["id"]
        row_start = time.time()

        try:
            treated = treat_row(row, config)
            insert_treated(conn, treated, config)
            update_treatment_status(conn, row_id, "success", config)
            conn.commit()

            duration = int((time.time() - row_start) * 1000)
            log_to_db(conn, "row_processed", config, source_row_id=row_id,
                       status="success", duration_ms=duration, batch_id=batch_id)
            conn.commit()

            success_count += 1

        except Exception as e:
            conn.rollback()
            logger.error(f"[pagamentos] Row {row_id}: {e}")

            try:
                update_treatment_status(conn, row_id, "failure", config)
                duration = int((time.time() - row_start) * 1000)
                log_to_db(conn, "row_failed", config, source_row_id=row_id,
                           status="failure", message=str(e), duration_ms=duration,
                           batch_id=batch_id)
                conn.commit()
            except Exception as e2:
                conn.rollback()
                logger.error(f"[pagamentos] Failed to log failure for row {row_id}: {e2}")

            failure_count += 1

    batch_duration = int((time.time() - batch_start) * 1000)
    logger.info(f"[pagamentos] Batch {batch_id[:8]}: Done — {success_count} ok, {failure_count} fail, {batch_duration}ms")

    log_to_db(conn, "batch_complete", config, batch_id=batch_id,
              rows_processed=success_count + failure_count,
              duration_ms=batch_duration,
              message=f"success={success_count} failure={failure_count}")
    conn.commit()

    return len(rows)



# ============================================================
# Main loop
# ============================================================

def main():
    config = load_config()
    logger = setup_logging(config)
    interval = config["processing"]["check_interval_seconds"]

    logger.info("Cleaner agent started")
    logger.info(f"Polling every {interval}s")

    while True:
        try:
            conn = get_connection(config)

            p_count = process_pagamentos_batch(conn, config, logger)
            if p_count == 0:
                logger.debug("[pagamentos] No untreated rows, sleeping...")

            conn.close()

        except Exception as e:
            logger.error(f"Connection/batch error: {e}")

        time.sleep(interval)

if __name__ == "__main__":
    main()
