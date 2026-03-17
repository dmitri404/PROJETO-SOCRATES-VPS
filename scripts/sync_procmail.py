#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_procmail.py
Le remetentes autorizados da tabela conf_emails (pode_disparar=TRUE)
do Supabase via docker exec e atualiza o procmailrc.
"""

import os
import sys
import subprocess
from datetime import datetime

PROCMAILRC = '/root/.procmailrc'
LOG        = '/opt/portal/logs/sync_procmail.log'
PLATAO     = '/opt/portal/scripts/platao.sh'

TEMPLATE = """\
SHELL=/bin/bash
LOGFILE=/opt/portal/logs/procmail.log
VERBOSE=yes

:0
* ^From:.*({emails})
* ^Subject:.*atualizar portal
| {platao}
"""

def log(msg):
    linha = f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] {msg}"
    print(linha)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, 'a') as f:
        f.write(linha + '\n')

def ler_remetentes():
    resultado = subprocess.run(
        ['docker', 'exec', 'supabase-db', 'psql', '-U', 'postgres',
         '-t', '-A', '-c',
         "SELECT email FROM conf_emails WHERE ativo=TRUE AND pode_disparar=TRUE ORDER BY id"],
        capture_output=True, text=True, timeout=15
    )
    if resultado.returncode != 0:
        raise RuntimeError(f"Erro psql: {resultado.stderr.strip()}")
    emails = [linha.strip() for linha in resultado.stdout.strip().splitlines() if linha.strip()]
    return emails

def escapar_email(email):
    return email.replace('.', r'\.')

def atualizar_procmailrc(emails):
    emails_regex = '|'.join(escapar_email(e) for e in emails)
    conteudo = TEMPLATE.format(emails=emails_regex, platao=PLATAO)

    if os.path.exists(PROCMAILRC) and os.path.isfile(PROCMAILRC):
        with open(PROCMAILRC, 'r') as f:
            atual = f.read()
        if atual == conteudo:
            log('Sem alteracoes — procmailrc ja esta atualizado.')
            return False

    with open(PROCMAILRC, 'w') as f:
        f.write(conteudo)
    return True

def main():
    log('Iniciando sincronizacao do procmailrc...')
    try:
        emails = ler_remetentes()
        if not emails:
            log('AVISO: Nenhum email com pode_disparar=TRUE — procmailrc nao alterado.')
            sys.exit(1)

        log(f'Remetentes autorizados ({len(emails)}): {emails}')
        atualizado = atualizar_procmailrc(emails)

        if atualizado:
            log('procmailrc atualizado com sucesso.')
        log('Sincronizacao concluida.')

    except Exception as e:
        log(f'ERRO: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main()
