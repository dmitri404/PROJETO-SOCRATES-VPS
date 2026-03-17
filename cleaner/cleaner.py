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
from datetime import datetime, timedelta, timezone

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

    m = re.search(r'(?:NF(?:S-?[eE]|SE)?|[Nn]ota\s+[Ff]is[cx]al(?:\s+de\s+Servi[çc]o)?)\s*(?::?\s*N?[ºo°\.]*\s*)(\d+)', value)
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
# Job 2: SASI Evaluations Parser
# ============================================================

def extract_field(data_fields, field_name):
    """Extract a field's formattedValue from the data_fields JSON array."""
    if not data_fields:
        return None
    for field in data_fields:
        if field.get("name") == field_name:
            return field.get("formattedValue") or field.get("value") or None
    return None


def resolve_data_registro(dia_de_registro, generated_at):
    """
    Resolve 'Hoje'/'Ontem' relative to generated_at timestamp.
    Returns a date string (YYYY-MM-DD).
    """
    if not generated_at:
        return None

    if isinstance(generated_at, str):
        # Parse ISO timestamp
        generated_at = generated_at.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(generated_at)
        except ValueError:
            return None
    else:
        dt = generated_at

    raw = (dia_de_registro or "").lower().strip()
    if raw in ("ontem", "yesterday"):
        dt = dt - timedelta(days=1)

    return dt.strftime("%Y-%m-%d")


def fetch_unprocessed_evaluations(conn, eval_config):
    """Fetch sasi_messages from evaluation channel not yet in mn_daily_evaluations."""
    channel = eval_config["channel_name"]
    batch_size = eval_config.get("batch_size", 50)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT m.id, m.sasi_message_id, m.profile_id, m.profile_name,
                   m.profile_email, m.generated_at, m.data_fields, m.raw_payload
            FROM sasi_messages m
            WHERE m.channel_name = %s
              AND NOT EXISTS (
                  SELECT 1 FROM mn_daily_evaluations e
                  WHERE e.source_event_id = m.id
              )
            ORDER BY m.generated_at ASC
            LIMIT %s
        """, (channel, batch_size))
        return cur.fetchall()


def lookup_worker(conn, profile_email):
    """Look up worker CPF by email from mn_mobile_users_contract."""
    if not profile_email:
        return None
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT cpf FROM mn_mobile_users_contract WHERE lower(email) = lower(%s) LIMIT 1",
            (profile_email,)
        )
        row = cur.fetchone()
        return row["cpf"] if row else None


def insert_evaluation(conn, evaluation):
    """Insert a single evaluation into mn_daily_evaluations."""
    columns = list(evaluation.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO mn_daily_evaluations ({col_names}) VALUES ({placeholders})",
            [evaluation[c] for c in columns]
        )


def process_evaluations_batch(conn, config, logger):
    """Process SASI messages into evaluations."""
    eval_config = config.get("evaluations")
    if not eval_config or not eval_config.get("enabled", False):
        return 0

    rows = fetch_unprocessed_evaluations(conn, eval_config)
    if not rows:
        return 0

    tenant_id = eval_config["tenant_id"]
    success_count = 0
    failure_count = 0

    logger.info(f"[evaluations] Processing {len(rows)} new SASI messages")

    for row in rows:
        try:
            data_fields = row["data_fields"]

            # Parse JSON if stored as string
            if isinstance(data_fields, str):
                data_fields = json.loads(data_fields)

            # Extract form fields
            dia_de_registro = extract_field(data_fields, "dia_de_registro")
            trabalhei = extract_field(data_fields, "trabalhei")
            local = extract_field(data_fields, "local")
            produtividade = extract_field(data_fields, "produtividade")
            motivo = extract_field(data_fields, "motivo")
            obs = extract_field(data_fields, "obs")
            tempo_servico_prestado = extract_field(data_fields, "tempo_servico_prestado")
            tempo_estimado = extract_field(data_fields, "tempo_estimado")
            tempo_registrado = extract_field(data_fields, "tempo_registrado")

            # Resolve the actual date
            data_registro = resolve_data_registro(dia_de_registro, row["generated_at"])

            # Look up worker CPF
            cpf = lookup_worker(conn, row["profile_email"])

            evaluation = {
                "source_event_id": row["id"],
                "sasi_profile_id": row["profile_id"],
                "profile_name": row["profile_name"],
                "email": row["profile_email"],
                "cpf": cpf,
                "dia_de_registro": dia_de_registro,
                "data_registro": data_registro,
                "data_enviado": row["generated_at"],
                "trabalhei": trabalhei,
                "local": local,
                "produtividade": produtividade,
                "motivo": motivo or None,
                "obs": obs or None,
                "tempo_servico_prestado": tempo_servico_prestado,
                "tempo_estimado": tempo_estimado or None,
                "tempo_registrado": tempo_registrado or None,
                "alert_id": row["sasi_message_id"],
                "raw_data": json.dumps(row["raw_payload"]) if row["raw_payload"] else None,
                "tenant_id": tenant_id,
            }

            insert_evaluation(conn, evaluation)
            conn.commit()

            success_count += 1
            logger.debug(f"[evaluations] Inserted: {row['profile_name']} — {data_registro}")

        except Exception as e:
            conn.rollback()
            logger.error(f"[evaluations] Message {row['id']}: {e}")
            failure_count += 1

    logger.info(f"[evaluations] Done — {success_count} ok, {failure_count} fail")

    # Log summary to cleaner_log
    try:
        log_to_db(conn, "eval_batch", config,
                  status="success" if failure_count == 0 else "partial",
                  rows_processed=success_count + failure_count,
                  message=f"evaluations: success={success_count} failure={failure_count}")
        conn.commit()
    except Exception:
        conn.rollback()

    return success_count + failure_count


# ============================================================
# Main loop
# ============================================================

def main():
    config = load_config()
    logger = setup_logging(config)
    interval = config["processing"]["check_interval_seconds"]

    logger.info("Cleaner agent started")
    logger.info(f"Polling every {interval}s")

    eval_enabled = config.get("evaluations", {}).get("enabled", False)
    if eval_enabled:
        logger.info(f"[evaluations] Enabled — channel: {config['evaluations']['channel_name']}")

    while True:
        try:
            conn = get_connection(config)

            # Job 1: Pagamentos
            p_count = process_pagamentos_batch(conn, config, logger)
            if p_count == 0:
                logger.debug("[pagamentos] No untreated rows, sleeping...")

            # Job 2: Evaluations
            if eval_enabled:
                e_count = process_evaluations_batch(conn, config, logger)
                if e_count == 0:
                    logger.debug("[evaluations] No new messages, sleeping...")

            conn.close()

        except Exception as e:
            logger.error(f"Connection/batch error: {e}")

        time.sleep(interval)

if __name__ == "__main__":
    main()
