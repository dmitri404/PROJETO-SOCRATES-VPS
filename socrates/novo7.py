import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

from playwright.sync_api import sync_playwright
import psycopg2
import time
from datetime import datetime
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# =================================================
# CONFIGURAÇÃO
# =================================================
LIMITE_POR_PAGINA = 100
MESES = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
         "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]


# Delays otimizados (em segundos)
DELAYS = {
    'inicial': 3,
    'navegacao': 1.9,
    'angular': 1.2,
    'modal': 0.9,
    'listagem': 1,
    'retry': 2
}

# Configurações de retry
MAX_TENTATIVAS = 3
TIMEOUT_CLICK = 10000  # 10 segundos por clique

# =================================================
# SUPABASE
# =================================================
def conectar_supabase():
    return psycopg2.connect(
        host=os.getenv('SUPABASE_DB_HOST', 'supabase-db'),
        port=int(os.getenv('SUPABASE_DB_PORT', '5432')),
        dbname=os.getenv('SUPABASE_DB_NAME', 'postgres'),
        user=os.getenv('SUPABASE_DB_USER', 'postgres'),
        password=os.getenv('SUPABASE_DB_PASSWORD'),
        connect_timeout=10
    )

def carregar_configuracoes():
    print("\n[CONF] Lendo configuracoes do Supabase...")
    try:
        conn = conectar_supabase()
        cur = conn.cursor()
        cur.execute("SELECT url_base, modo_limpar FROM conf LIMIT 1")
        row = cur.fetchone()
        if not row:
            raise ValueError("Nenhuma configuracao na tabela conf")
        url_base, modo_limpar = row
        if not url_base.endswith('/'):
            url_base += '/'
        cur.execute("SELECT cpf_cnpj FROM conf_cpfs WHERE ativo=TRUE ORDER BY id")
        cpfs = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT exercicio FROM conf_exercicios WHERE ativo=TRUE ORDER BY id")
        exercicios = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT email FROM conf_emails WHERE ativo=TRUE ORDER BY id")
        emails = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        if not cpfs:
            raise ValueError("Nenhum CPF/CNPJ ativo")
        if not exercicios:
            raise ValueError("Nenhum exercicio ativo")
        print(f"[CONF] URL: {url_base} | Modo Limpar: {modo_limpar}")
        print(f"[CONF] CPFs: {cpfs} | Exercicios: {exercicios}")
        return url_base, cpfs, exercicios, modo_limpar, emails
    except Exception as e:
        print(f"[CONF] ERRO: {e}")
        raise

def carregar_empenhos_existentes():
    print("\n[DB] Carregando empenhos existentes...")
    try:
        conn = conectar_supabase()
        cur = conn.cursor()
        cur.execute("SELECT empenho FROM empenhos")
        existentes = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        print(f"[DB] {len(existentes)} empenhos existentes")
        return existentes
    except Exception as e:
        print(f"[DB] ERRO: {e}")
        return set()

def carregar_pagamentos_existentes():
    print("\n[DB] Carregando pagamentos existentes...")
    try:
        conn = conectar_supabase()
        cur = conn.cursor()
        cur.execute("SELECT empenho || '|' || pagamento FROM pagamentos")
        existentes = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        print(f"[DB] {len(existentes)} pagamentos existentes")
        return existentes
    except Exception as e:
        print(f"[DB] ERRO: {e}")
        return set()

def salvar_empenhos(rows):
    if not rows:
        print("[DB] Nenhum empenho novo")
        return
    print(f"[DB] Salvando {len(rows)} empenhos...")
    try:
        conn = conectar_supabase()
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO empenhos (empenho,descricao,orgao,unidade,programa,credor,data,empenhado,liquidado,pago,anulado_empenho,pagamento_anulado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (empenho) DO NOTHING",
            rows
        )
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Empenhos salvos")
    except Exception as e:
        print(f"[DB] ERRO ao salvar empenhos: {e}")

def salvar_pagamentos(rows):
    if not rows:
        print("[DB] Nenhum pagamento novo")
        return
    print(f"[DB] Salvando {len(rows)} pagamentos...")
    try:
        conn = conectar_supabase()
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO pagamentos (empenho,pagamento,data,valor,descricao) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (empenho,pagamento) DO NOTHING",
            rows
        )
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Pagamentos salvos")
    except Exception as e:
        print(f"[DB] ERRO ao salvar pagamentos: {e}")

def salvar_log_execucao(inicio, fim, duracao, empenhos_novos, pagamentos_novos,
                        cpfs, exercicios, meses, modo_limpar):
    print("\n[DB] Salvando log...")
    try:
        conn = conectar_supabase()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO execucao_logs (inicio,fim,duracao,empenhos_novos,pagamentos_novos,cpfs,exercicios,meses,modo,combinacoes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (inicio, fim, str(duracao), empenhos_novos, pagamentos_novos,
             ', '.join(cpfs), ', '.join(map(str, exercicios)), ', '.join(meses),
             'LIMPEZA' if modo_limpar else 'APPEND',
             len(cpfs) * len(exercicios) * len(meses))
        )
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Log salvo")
    except Exception as e:
        print(f"[DB] ERRO ao salvar log: {e}")

# =================================================
# E-MAIL
# =================================================
def enviar_email_resumo(destinatarios, inicio, fim, duracao,
                        empenhos_novos, pagamentos_novos, cpfs, exercicios):
    """Envia e-mail com resumo da execucao para os destinatarios da coluna E da aba 'conf'"""
    if not destinatarios:
        print("\n[EMAIL] \u26a0 Nenhum destinatario encontrado na coluna E da aba 'conf' - e-mail nao enviado")
        return

    remetente = "eliotsafadao@gmail.com"
    try:
        import json as _json
        with open("credentials.json", "r") as _f:
            _creds = _json.load(_f)
        senha = _creds.get("gmail_app_password", "")
    except Exception as _e:
        print(f"[EMAIL] Nao foi possivel ler credentials.json: {_e}")
        senha = ""

    if not senha:
        print("[EMAIL] Campo gmail_app_password nao encontrado em credentials.json - e-mail nao enviado")
        return

    assunto = f"[Transparencia Manaus] Execucao concluida - {fim.strftime('%d/%m/%Y %H:%M')}"

    corpo = (
        "Resumo da Execucao - Scraper Transparencia Manaus\n"
        "==================================================\n\n"
        f"Data/Hora Inicio : {inicio.strftime('%d/%m/%Y %H:%M:%S')}\n"
        f"Data/Hora Fim    : {fim.strftime('%d/%m/%Y %H:%M:%S')}\n"
        f"Duracao          : {duracao}\n\n"
        "Resultados:\n"
        f"  Empenhos novos   : {empenhos_novos}\n"
        f"  Pagamentos novos : {pagamentos_novos}\n\n"
        f"CPFs/CNPJs processados : {chr(44).join(cpfs)}\n"
        f"Exercicios             : {chr(44).join(map(str, exercicios))}\n\n"
        "Planilha: Transparencia Manaus - Despesas\n"
        "--------------------------------------------------\n"
        "Mensagem automatica gerada pelo scraper."
    )

    try:
        msg = MIMEMultipart()
        msg['From'] = remetente
        msg['To'] = ', '.join(destinatarios)
        msg['Subject'] = assunto
        msg.attach(MIMEText(corpo, 'plain', 'utf-8'))

        print(f"\n[EMAIL] Enviando e-mail para {len(destinatarios)} destinatario(s): {destinatarios}")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(remetente, senha)
            smtp.sendmail(remetente, destinatarios, msg.as_string())

        print("[EMAIL] \u2713 E-mail enviado com sucesso!")
    except Exception as e:
        print(f"[EMAIL] \u2717 Erro ao enviar e-mail: {e}")

# =================================================
# PLAYWRIGHT HELPERS
# =================================================
def angular_select(page, selector, value):
    """Seleciona opção em dropdown Angular"""
    print(f"[ANGULAR] Selecionando '{value}' em '{selector}'")
    try:
        page.wait_for_selector(selector, timeout=5000)
        page.select_option(selector, value=value)
        page.locator(selector).dispatch_event("change")
        time.sleep(DELAYS['angular'])
        print(f"[ANGULAR] ✓ Seleção concluída")
    except Exception as e:
        print(f"[ANGULAR] ✗ Erro: {e}")
        raise

def esperar_modal_abrir(page, timeout=10000):
    """Espera o modal abrir com mais tolerância e verificação robusta"""
    try:
        # Aguardar até que o modal esteja realmente visível e estável
        page.wait_for_selector('.modal.in, .modal.show, div[role="dialog"]', 
                              timeout=timeout, 
                              state='visible')
        
        # Aguardar um pouco mais para o conteúdo carregar
        time.sleep(0.5)
        
        # Verificar se realmente tem conteúdo
        content = page.locator('.modal-content').first
        if content.count() > 0:
            return True
        
        return False
    except Exception as e:
        print(f"[WAIT MODAL] ⚠ Timeout ou erro: {e}")
        return False

def fechar_modals_abertos(page):
    """Fecha TODOS os modals que estiverem abertos na página de forma agressiva"""
    try:
        modals_fechados = 0
        max_tentativas = 3
        
        for tentativa in range(max_tentativas):
            # 1. Tentar fechar com botões visíveis
            try:
                botoes_fechar = page.locator(
                    '.modal .close, '
                    'button[data-dismiss="modal"], '
                    'button:has-text("Fechar"), '
                    'button:has-text("Cancelar"), '
                    'button[ng-click="cancel()"], '
                    '.modal-footer button:first-child'
                )
                
                count = botoes_fechar.count()
                for i in range(count):
                    try:
                        botoes_fechar.nth(i).click(timeout=1000, force=True)
                        modals_fechados += 1
                        time.sleep(0.1)
                    except:
                        pass
            except:
                pass
            
            # 2. Pressionar ESC múltiplas vezes
            for _ in range(3):
                try:
                    page.keyboard.press('Escape')
                    time.sleep(0.1)
                except:
                    pass
            
            # 3. Forçar remoção via JavaScript (último recurso)
            try:
                removed = page.evaluate("""
                    () => {
                        let count = 0;
                        // Remover modals
                        document.querySelectorAll('.modal, div[role="dialog"]').forEach(m => {
                            m.remove();
                            count++;
                        });
                        // Remover backdrops
                        document.querySelectorAll('.modal-backdrop').forEach(b => {
                            b.remove();
                            count++;
                        });
                        // Restaurar body
                        document.body.classList.remove('modal-open');
                        document.body.style.overflow = '';
                        document.body.style.paddingRight = '';
                        return count;
                    }
                """)
                if removed > 0:
                    modals_fechados += removed
            except:
                pass
            
            # Verificar se ainda há modals
            modals_restantes = page.evaluate("""
                () => {
                    return document.querySelectorAll('.modal, .modal-backdrop, div[role="dialog"]').length;
                }
            """)
            
            if modals_restantes == 0:
                break
            
            time.sleep(0.3)
        
        if modals_fechados > 0:
            print(f"[MODAL] ✓ {modals_fechados} tentativa(s) de fechamento realizada(s)")
        
        return modals_fechados > 0
        
    except Exception as e:
        print(f"[MODAL] ⚠ Erro ao tentar fechar modals: {e}")
        # Última tentativa desesperada
        try:
            page.evaluate("""
                () => {
                    document.querySelectorAll('body > *').forEach(el => {
                        if (el.style.display === 'block' || 
                            el.classList.contains('modal') ||
                            el.classList.contains('modal-backdrop')) {
                            el.remove();
                        }
                    });
                    document.body.style.overflow = 'auto';
                }
            """)
        except:
            pass
        return False

def clicar_com_retry(page, seletor, descricao="elemento", tentativas=MAX_TENTATIVAS):
    """Tenta clicar em um elemento com retry e tratamento de modals"""
    for tentativa in range(1, tentativas + 1):
        try:
            print(f"[CLICK] Tentando clicar em '{descricao}' (tentativa {tentativa}/{tentativas})")
            
            # SEMPRE fechar modals antes de qualquer clique
            fechar_modals_abertos(page)
            
            # Esperar elemento estar pronto
            page.wait_for_selector(seletor, timeout=5000, state='visible')
            
            # Tentar clicar com force=True para ignorar sobreposições
            page.click(seletor, timeout=TIMEOUT_CLICK, force=True)
            print(f"[CLICK] ✓ Clique realizado com sucesso")
            
            # Pequeno delay após clique bem-sucedido
            time.sleep(0.5)
            return True
            
        except Exception as e:
            erro_str = str(e)
            print(f"[CLICK] ⚠ Tentativa {tentativa} falhou: {erro_str[:150]}")
            
            # Se detectar modal no erro, tentar fechamento mais agressivo
            if 'modal' in erro_str.lower() or 'dialog' in erro_str.lower():
                print(f"[CLICK] Modal detectado no erro - fechamento agressivo...")
                for _ in range(3):
                    fechar_modals_abertos(page)
                    page.keyboard.press('Escape')
                    time.sleep(0.3)
            
            if tentativa < tentativas:
                print(f"[CLICK] Aguardando {DELAYS['retry']}s antes de tentar novamente...")
                time.sleep(DELAYS['retry'])
            else:
                print(f"[CLICK] ✗ Todas as tentativas falharam para '{descricao}'")
                return False
    
    return False

def esperar_carregamento(page, selector, timeout=10000):
    """Espera elemento aparecer na página"""
    try:
        page.wait_for_selector(selector, timeout=timeout)
        return True
    except:
        print(f"[WAIT] ⚠ Timeout ao aguardar: {selector}")
        return False

# =================================================
# FUNÇÕES DE PROCESSAMENTO DE PAGAMENTOS
# =================================================
def processar_pagamentos_modal(page, empenho_num, pagamentos_existentes):
    """Função dedicada para processar pagamentos em modal com tratamento robusto"""
    pagamentos_rows = []
    estatisticas = {'novos': 0, 'duplicados': 0, 'erros': 0}
    
    # Localizar links de pagamentos
    links_pag = page.locator('a[ng-click="abrirDetalhesPag(pag.PagNumero)"]')
    total_pag = links_pag.count()
    
    print(f"[PAGAMENTOS] {total_pag} pagamento(s) encontrado(s)")
    
    for i in range(total_pag):
        print(f"[PAGAMENTO {i+1}/{total_pag}] Processando...")
        
        # 1. FECHAR QUALQUER MODAL ANTES DE ABRIR NOVO
        fechar_modals_abertos(page)
        time.sleep(0.5)
        
        # 2. Tentar clicar no link do pagamento com múltiplas estratégias
        click_success = False
        for tentativa in range(1, 4):
            try:
                print(f"[PAGAMENTO {i+1}] Tentativa {tentativa}: Clicando no link...")
                
                # Verificar se há modais bloqueando
                modals = page.locator('.modal.in, .modal.show, div[role="dialog"]').count()
                if modals > 0:
                    print(f"[PAGAMENTO {i+1}] Modal detectado (tentativa {tentativa}) - fechando...")
                    fechar_modals_abertos(page)
                    time.sleep(0.5)
                
                # Tentar clique normal
                try:
                    links_pag.nth(i).click(timeout=5000)
                    click_success = True
                    break
                except:
                    # Se falhar, tentar com JavaScript
                    page.evaluate(f"""
                        (index) => {{
                            const links = document.querySelectorAll('a[ng-click="abrirDetalhesPag(pag.PagNumero)"]');
                            if (links[index]) {{
                                links[index].click();
                                return true;
                            }}
                            return false;
                        }}
                    """, i)
                    click_success = True
                    break
                    
            except Exception as e:
                print(f"[PAGAMENTO {i+1}] ⚠ Tentativa {tentativa} falhou: {str(e)[:100]}")
                if tentativa < 3:
                    time.sleep(1)
                else:
                    estatisticas['erros'] += 1
                    print(f"[PAGAMENTO {i+1}] ✗ Não foi possível abrir modal após 3 tentativas")
                    continue
        
        if not click_success:
            continue
        
        # 3. AGUARDAR MODAL ABRIR (com timeout maior)
        if not esperar_modal_abrir(page, timeout=8000):
            print(f"[PAGAMENTO {i+1}] ⚠ Modal não abriu após 8 segundos - pulando")
            fechar_modals_abertos(page)
            continue
        
        # 4. EXTRAIR DADOS DO MODAL
        pag = None
        try:
            pag = page.evaluate("""
                () => {
                    // Verificar múltiplos seletores para robustez
                    const selectors = [
                        ".modal-content h4",
                        ".modal-header h4",
                        ".modal-title",
                        "h4.modal-title"
                    ];
                    
                    let numero = "";
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el && el.innerText.trim()) {
                            numero = el.innerText.trim();
                            break;
                        }
                    }
                    
                    // Buscar parágrafos
                    const paragraphs = Array.from(document.querySelectorAll(".modal-content p"))
                        .map(p => p.innerText.trim())
                        .filter(p => p);
                    
                    return {
                        numero: numero,
                        data: paragraphs[0] || "",
                        valor: paragraphs[1] || "",
                        descricao: paragraphs[2] || ""
                    };
                }
            """)
        except Exception as e:
            print(f"[PAGAMENTO {i+1}] ⚠ Erro ao extrair dados: {e}")
            fechar_modals_abertos(page)
            continue
        
        if not pag or not pag['numero']:
            print(f"[PAGAMENTO {i+1}] ⚠ Dados do modal vazios ou inválidos")
            fechar_modals_abertos(page)
            continue
        
        # 5. VERIFICAR DUPLICAÇÃO E SALVAR
        chave_pagamento = f"{empenho_num}|{pag['numero']}"
        
        if chave_pagamento in pagamentos_existentes:
            print(f"[PAGAMENTO {i+1}] ⊘ JÁ EXISTE - PULANDO")
            estatisticas['duplicados'] += 1
        else:
            pagamentos_rows.append([
                empenho_num,
                pag["numero"],
                pag["data"],
                pag["valor"],
                pag["descricao"]
            ])
            estatisticas['novos'] += 1
            print(f"[PAGAMENTO {i+1}] ✓ NOVO - Coletado - Valor: {pag['valor']}")
        
        # 6. FECHAR MODAL DE FORMA ROBUSTA
        print(f"[PAGAMENTO {i+1}] Fechando modal...")
        fechar_modals_abertos(page)
        
        # Pequena pausa entre pagamentos
        time.sleep(0.5)
    
    return pagamentos_rows, estatisticas

# =================================================
# SCRAPING
# =================================================
def processar_cpf_cnpj(page, cpf_cnpj, exercicio, mes, empenhos_existentes, pagamentos_existentes, url_base):
    """Processa um CPF/CNPJ específico para um exercício e mês"""
    print(f"\n{'='*60}")
    print(f"[PROCESSAMENTO] CPF/CNPJ: {cpf_cnpj}")
    print(f"[PROCESSAMENTO] Exercício: {exercicio} | Mês: {mes}")
    print(f"{'='*60}")
    
    meses = {
        "Janeiro": "number:1", "Fevereiro": "number:2", "Março": "number:3",
        "Abril": "number:4", "Maio": "number:5", "Junho": "number:6",
        "Julho": "number:7", "Agosto": "number:8", "Setembro": "number:9",
        "Outubro": "number:10", "Novembro": "number:11", "Dezembro": "number:12"
    }

    empenhos_rows = []
    pagamentos_rows = []
    empenhos_processados = set()
    
    estatisticas = {
        'total_portal': 0,
        'duplicados': 0,
        'novos': 0,
        'pagamentos_novos': 0,
        'pagamentos_duplicados': 0
    }

    try:
        # Navegar para busca de credores
        print(f"\n[NAVEGAÇÃO] Indo para página de despesas...")
        
        # Tentar ir para home primeiro
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                # Limpar tudo antes de navegar
                fechar_modals_abertos(page)
                
                page.goto(f"{url_base}#/home",
                         timeout=30000,
                         wait_until='domcontentloaded')
                time.sleep(DELAYS['inicial'])
                
                # Fechar TODOS os modals que possam ter aparecido no carregamento
                for _ in range(3):
                    if fechar_modals_abertos(page):
                        time.sleep(0.5)
                
                print(f"[NAVEGAÇÃO] ✓ Home carregada e modals limpos")
                break
            except Exception as e:
                print(f"[NAVEGAÇÃO] ⚠ Tentativa {tentativa} de carregar home falhou: {str(e)[:100]}")
                if tentativa == MAX_TENTATIVAS:
                    raise

        # Clicar em Despesa com retry
        if not clicar_com_retry(page, 'img[alt="Despesa"]', "botão Despesa"):
            print(f"[NAVEGAÇÃO] ✗ Não foi possível acessar Despesas - PULANDO CPF/CNPJ")
            return [], []
        
        page.wait_for_url("**/despesas", timeout=10000)
        print(f"[NAVEGAÇÃO] ✓ Página de despesas carregada")

        # Buscar credor
        print(f"\n[BUSCA] Preenchendo CPF/CNPJ: {cpf_cnpj}")
        
        if not clicar_com_retry(page, 'li[ng-click="selecionarAba(1)"]', "aba de busca"):
            return [], []
        
        time.sleep(DELAYS['navegacao'])
        
        page.fill("#inputCpfCnpj", cpf_cnpj)
        page.click("#cnscredores")
        time.sleep(DELAYS['navegacao'])

        if not esperar_carregamento(page, 'a[href^="#/credoresempenho/"]', 5000):
            print(f"[BUSCA] ⚠ Nenhum resultado encontrado para {cpf_cnpj}")
            return empenhos_rows, pagamentos_rows

        page.click('a[href^="#/credoresempenho/"]')
        print(f"[BUSCA] ✓ Credor encontrado e selecionado")

        # Filtros
        print(f"\n[FILTROS] Aplicando Exercício: {exercicio} | Mês: {mes}")
        angular_select(page, 'select[ng-model="exercicio"]', f"number:{exercicio}")
        angular_select(page, 'select[ng-model="mes"]', meses[mes])

        # Coletar links de empenhos
        print(f"\n[COLETA] Coletando links de empenhos...")
        pagina = 1
        todos_links = []

        while True:
            print(f"[COLETA] Página {pagina}...")
            
            page.evaluate(f"""
                () => {{
                    const s = angular.element(document.querySelector('button[ng-click^="cnsMovimento"]')).scope();
                    s.cnsMovimento(s.orgao, s.unidade, s.FornID,
                        s.tipomovimento, s.exercicio, s.mes,
                        {pagina}, {LIMITE_POR_PAGINA}, 0, '');
                    s.$apply();
                }}
            """)
            time.sleep(DELAYS['listagem'])

            links = page.evaluate("""
                () => Array.from(document.querySelectorAll('td.number a'))
                    .map(a => a.getAttribute('href'))
            """)

            if not links:
                print(f"[COLETA] Nenhum link encontrado na página {pagina}")
                break

            todos_links.extend(links)
            print(f"[COLETA] ✓ {len(links)} links coletados (Total: {len(todos_links)})")
            
            if len(links) < LIMITE_POR_PAGINA:
                print(f"[COLETA] Última página atingida")
                break
            pagina += 1

        lista_url = page.url
        estatisticas['total_portal'] = len(todos_links)
        print(f"\n[COLETA] Total de empenhos encontrados no portal: {len(todos_links)}")
        print(f"[DEDUP] Verificação contra {len(empenhos_existentes)} empenhos já salvos...")

        # =================================================
        # PROCESSAR CADA EMPENHO
        # =================================================
        for idx, href in enumerate(todos_links, 1):
            detalhe_url = f"{url_base}{href}"
            print(f"\n[EMPENHO {idx}/{len(todos_links)}] Acessando detalhes...")
            
            page.goto(detalhe_url)
            time.sleep(DELAYS['navegacao'])

            if not esperar_carregamento(page, ".descricao_page b.ng-binding"):
                print(f"[EMPENHO {idx}] ⚠ Erro ao carregar página")
                continue

            # Extrair dados do empenho
            detalhe = page.evaluate("""
                () => {
                    const get = sel => document.querySelector(sel)?.innerText.trim() || "";
                    const byLabel = txt => {
                        const l = [...document.querySelectorAll("label")]
                            .find(x => x.innerText.startsWith(txt));
                        return l?.nextElementSibling?.innerText.trim() || "";
                    };

                    return {
                        empenho: get(".descricao_page b.ng-binding"),
                        descricao: get("#c2"),
                        orgao: get("#c3"),
                        unidade: byLabel("Unidade"),
                        programa: byLabel("Programa"),
                        credor: byLabel("Credor"),
                        data: byLabel("Data"),
                        empenhado: byLabel("Empenhado"),
                        liquidado: byLabel("Liquidado"),
                        pago: byLabel("Pago"),
                        anuladoEmpenho: byLabel("Anulado Empenho"),
                        pagamentoAnulado: byLabel("Pagamento Anulado")
                    };
                }
            """)

            num_empenho = detalhe["empenho"]
            print(f"[EMPENHO {idx}] Número: {num_empenho}")

            # Verificar se EMPENHO já existe
            empenho_ja_existe = num_empenho in empenhos_existentes or num_empenho in empenhos_processados
            
            if empenho_ja_existe:
                print(f"[EMPENHO {idx}] ⊘ JÁ EXISTE - Salvando apenas PAGAMENTOS NOVOS")
                estatisticas['duplicados'] += 1
            else:
                # Empenho é NOVO - salvar dados do empenho
                empenhos_processados.add(num_empenho)
                estatisticas['novos'] += 1
                
                empenhos_rows.append([
                    detalhe["empenho"], detalhe["descricao"], detalhe["orgao"],
                    detalhe["unidade"], detalhe["programa"], detalhe["credor"], 
                    detalhe["data"], detalhe["empenhado"], detalhe["liquidado"], 
                    detalhe["pago"], detalhe["anuladoEmpenho"], detalhe["pagamentoAnulado"]
                ])
                print(f"[EMPENHO {idx}] ✓ NOVO - Dados coletados - Credor: {detalhe['credor'][:30]}...")

            # =================================================
            # PROCESSAR PAGAMENTOS COM FUNÇÃO DEDICADA
            # =================================================
            pagamentos_rows_lote, stats_pag = processar_pagamentos_modal(
                page, num_empenho, pagamentos_existentes
            )
            pagamentos_rows.extend(pagamentos_rows_lote)
            
            # Atualizar estatísticas
            estatisticas['pagamentos_novos'] += stats_pag['novos']
            estatisticas['pagamentos_duplicados'] += stats_pag['duplicados']

            # Voltar para lista
            page.goto(lista_url)
            time.sleep(DELAYS['navegacao'])

        # Resumo do CPF/CNPJ
        print(f"\n{'='*60}")
        print(f"[RESUMO] {cpf_cnpj} - {exercicio}/{mes}")
        print(f"  • Total no portal: {estatisticas['total_portal']}")
        print(f"  • Duplicados (já salvos): {estatisticas['duplicados']}")
        print(f"  • Novos empenhos: {estatisticas['novos']}")
        print(f"  • Novos pagamentos: {estatisticas['pagamentos_novos']}")
        print(f"  • Pagamentos duplicados: {estatisticas['pagamentos_duplicados']}")
        print(f"{'='*60}")

        return empenhos_rows, pagamentos_rows

    except Exception as e:
        print(f"\n[ERRO] Exceção ao processar {cpf_cnpj}: {e}")
        return empenhos_rows, pagamentos_rows

# =================================================
# MAIN
# =================================================
def executar():
    """Função principal"""
    inicio = datetime.now()
    print(f"\n{'#'*60}")
    print(f"# SCRAPER TRANSPARÊNCIA MANAUS")
    print(f"# Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'#'*60}")

    url_base, CPFS_CNPJS, EXERCICIOS, MODO_LIMPAR_PLANILHA, EMAILS_DESTINATARIOS = carregar_configuracoes()


    # Carregar dados existentes para deduplicação (se não limpar)
    if MODO_LIMPAR_PLANILHA:
        print("\n[MODO] ⚠️  LIMPEZA ATIVADA - Planilha será LIMPA NO INÍCIO")
        print("[MODO] ⚠️  Mas deduplicação CONTINUA ATIVA durante a execução")
        print("[MODO] ⚠️  (evita duplicados entre combinações CPF/Exercício/Mês)")
        empenhos_existentes = set()
        pagamentos_existentes = set()
    else:
        print("\n[MODO] ✓ APPEND ATIVADO - Dados antigos preservados")
        print("[MODO] ✓ Apenas novos dados serão adicionados")
        empenhos_existentes = carregar_empenhos_existentes()
        pagamentos_existentes = carregar_pagamentos_existentes()

    # Iniciar navegador
    print(f"\n[BROWSER] Iniciando navegador...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=100)
        page = browser.new_page()
        print(f"[BROWSER] ✓ Navegador iniciado")

        todos_empenhos = []
        todos_pagamentos = []

        # Processar todas as combinações: CPF x EXERCÍCIO x MÊS (dinâmico)
        hoje = datetime.now()
        ano_atual = hoje.year
        mes_atual = hoje.month  # 1-12

        # Calcular total de combinações e meses por exercício
        total_combinacoes = 0
        for ex in EXERCICIOS:
            ano_ex = int(ex)
            if ano_ex < ano_atual:
                total_combinacoes += len(CPFS_CNPJS) * 12
            else:
                total_combinacoes += len(CPFS_CNPJS) * min(mes_atual, 12)
        contador = 0

        print(f"\n{'#'*60}")
        print(f"# TOTAL DE COMBINAÇÕES A PROCESSAR: {total_combinacoes}")
        print(f"# CPFs: {len(CPFS_CNPJS)} | Exercícios: {len(EXERCICIOS)}")
        print(f"# Referência: {MESES[mes_atual - 1]}/{ano_atual} (mês/ano atual)")
        print(f"{'#'*60}")

        for cpf_cnpj in CPFS_CNPJS:
            for exercicio in EXERCICIOS:
                ano_ex = int(exercicio)
                if ano_ex < ano_atual:
                    meses_processar = MESES  # todos os 12
                else:
                    meses_processar = MESES[:mes_atual]  # até o mês atual (inclusive)

                print(f"\n[INFO] Exercício {exercicio}: processando {len(meses_processar)} meses "
                      f"({meses_processar[0]} a {meses_processar[-1]})")

                for mes in meses_processar:
                    contador += 1
                    print(f"\n[PROGRESSO] Combinação {contador}/{total_combinacoes}")
                    
                    empenhos, pagamentos = processar_cpf_cnpj(
                        page, cpf_cnpj, exercicio, mes,
                        empenhos_existentes, pagamentos_existentes, url_base
                    )
                    todos_empenhos.extend(empenhos)
                    todos_pagamentos.extend(pagamentos)
                    
                    # Atualizar sets de existentes para evitar duplicação entre combinações
                    for emp in empenhos:
                        empenhos_existentes.add(emp[0])  # Número do empenho
                    for pag in pagamentos:
                        pagamentos_existentes.add(f"{pag[0]}|{pag[1]}")  # Empenho|Pagamento

        browser.close()
        print(f"\n[BROWSER] ✓ Navegador fechado")

    # =================================================
    # SALVAR NO SUPABASE
    # =================================================
    salvar_empenhos(todos_empenhos)
    salvar_pagamentos(todos_pagamentos)

        # Resumo final
    fim = datetime.now()
    duracao = fim - inicio
    
    print(f"\n{'#'*60}")
    print(f"# EXECUÇÃO CONCLUÍDA")
    print(f"# Duração: {duracao}")
    print(f"# Empenhos novos: {len(todos_empenhos)}")
    print(f"# Pagamentos novos: {len(todos_pagamentos)}")
    print(f"# Fim: {fim.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'#'*60}\n")
    
    # Salvar log de execução
    salvar_log_execucao(
        inicio, 
        fim, 
        duracao,
        len(todos_empenhos),
        len(todos_pagamentos),
        CPFS_CNPJS,
        EXERCICIOS,
        MESES,
        MODO_LIMPAR_PLANILHA
    )

    # Enviar e-mail de resumo para destinatarios da coluna E da aba 'conf'
    enviar_email_resumo(
        EMAILS_DESTINATARIOS,
        inicio,
        fim,
        duracao,
        len(todos_empenhos),
        len(todos_pagamentos),
        CPFS_CNPJS,
        EXERCICIOS
    )

if __name__ == "__main__":
    executar()
