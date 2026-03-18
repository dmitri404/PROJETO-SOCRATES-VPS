# PROJETO-SOCRATES-VPS

Sistema integrado de captura, processamento e armazenamento de documentos fiscais e dados de transparência governamental.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura](#2-arquitetura)
3. [Fluxo de Dados](#3-fluxo-de-dados)
4. [Guia de Configuração](#4-guia-de-configuração)
5. [Fluxograma](#5-fluxograma)

---

## 1. Visão Geral

### Objetivo

O sistema automatiza o ciclo completo de tratamento de documentos fiscais emitidos para órgãos públicos municipais e estaduais do Amazonas. Ele resolve os seguintes problemas:

| Problema | Solução |
|---|---|
| Notas Fiscais em PDFs dispersos em pastas de rede | ProcessadorNF lê e estrutura os dados automaticamente |
| Credenciais do banco de dados expostas no cliente | Portal API intermediária — o cliente nunca toca o Supabase diretamente |
| Dados brutos de pagamentos no portal SEFAZ | Scrapers coletam e inserem no banco de forma automática |
| Dados de pagamentos sem tratamento para análise | Cleaner transforma e normaliza os registros |
| Múltiplos portais com schemas distintos | Cada portal tem seu próprio router, schema e chave de API |

### Escopo

- **Portal Municipal Manaus** — NFS-e emitidas para a Prefeitura de Manaus
- **Portal Estado AM** — Pagamentos e empenhos via portal de transparência SEFAZ/AM

---

## 2. Arquitetura

### Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Banco de Dados | PostgreSQL 15 via Supabase (self-hosted, porta 8000) |
| API HTTP | FastAPI + Uvicorn (porta 9000) |
| Cliente Desktop | Python 3.12 + Tkinter (Windows) |
| Scraping Web | Playwright (Chromium headless) |
| ETL | Python + psycopg2 (direto ao banco) |
| Infraestrutura | Docker + Docker Compose (VPS 187.77.240.80) |
| Extração de PDF | pdfplumber → PyPDF2 → Tesseract OCR (fallback em cadeia) |

### Componentes e Responsabilidades

```
┌──────────────────────────────────────────────────────────────────┐
│  CLIENTE (Windows)                                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ProcessadorNF (app.py)                                  │    │
│  │  GUI Tkinter · Multi-perfil · Autenticação SMB           │    │
│  │  Extração PDF · Envio via HTTP para Portal API           │    │
│  └──────────────────────────┬───────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────┘
                              │ HTTP POST x-api-key
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  VPS (Docker Compose)                                            │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  portal-api (FastAPI · porta 9000)                        │   │
│  │  ├── /portal-municipal-manaus/faturamento                 │   │
│  │  └── /portal-estado-am/faturamento (+ pagamentos, logs)  │   │
│  └──────────────────────────┬─────────────────────────────── ┘  │
│                             │                                    │
│  ┌──────────────────────────▼─────────────────────────────── ┐  │
│  │  PostgreSQL / Supabase (porta 5432 interna)                │  │
│  │  ├── schema: public         → faturamento (municipal)      │  │
│  │  └── schema: portal_estado_am → pagamentos, nl_itens, conf │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────┐  ┌───────────────┐  ┌──────────────────────┐ │
│  │  portal-      │  │  socrates     │  │  portal-cleaner      │ │
│  │  aristoteles  │  │  (Playwright) │  │  (ETL pagamentos)    │ │
│  │  PDF watcher  │  │  SEFAZ scraper│  │  pagamentos_treated  │ │
│  └───────────────┘  └───────────────┘  └──────────────────────┘ │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  portal-estado-am (Playwright · SEFAZ transparência)      │  │
│  └───────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Redes Docker

| Rede | Tipo | Membros |
|---|---|---|
| `portal_default` | bridge (interna) | Todos os serviços do portal |
| `supabase_default` | external (existente) | Serviços que acessam o banco |

### Comunicação entre Componentes

- **ProcessadorNF → portal-api**: HTTP REST com header `x-api-key`
- **portal-api → PostgreSQL**: psycopg2 direto via rede Docker (`supabase-db:5432`)
- **portal-aristoteles → PostgreSQL**: psycopg2 direto (sem passar pela API)
- **socrates / portal-estado-am → PostgreSQL**: psycopg2 direto
- **cleaner → PostgreSQL**: psycopg2 direto (leitura e escrita)

---

## 3. Fluxo de Dados

### 3.1 Processamento de Notas Fiscais (ProcessadorNF)

**Ator:** Usuário do setor administrativo (Windows)

```
1. Usuário abre ProcessadorNF.exe e seleciona um ou mais perfis
2. App autentica na pasta SMB via `net use` (usuário/senha de rede)
3. Lista todos os PDFs da pasta que NÃO estão em /processados ou /erro
4. Para cada PDF:
   a. Extrai texto: tenta pdfplumber → PyPDF2 → Tesseract OCR
   b. Detecta modelo do documento: DANFSE | NOTA | FATURA
   c. Extrai campos estruturados: número, data, CNPJs, valores
   d. Converte valores BR (1.234,56) para float
   e. Consulta API: GET /{portal}/faturamento/existe/{numero}
   f. Se já existe → move para /processados (duplicata, sem reinserção)
   g. Se novo    → POST /{portal}/faturamento com JSON dos dados
   h. Se sucesso → move para /processados
   i. Se erro    → move para /erro
5. Exibe relatório na interface: inseridos / duplicatas / erros
```

### 3.2 Inserção via Portal API

**Ator:** ProcessadorNF ou qualquer cliente HTTP autorizado

```
1. Cliente envia POST /{portal}/faturamento com x-api-key no header
2. auth.py valida a chave contra a variável de ambiente correspondente
3. Router estabelece conexão psycopg2 com supabase-db
4. Executa INSERT INTO faturamento ... ON CONFLICT (numero_nota) DO NOTHING
5. Verifica rowcount: 1 = inserido | 0 = duplicata
6. Retorna JSON: {"status": "inserido" | "duplicata", "numero_nota": "..."}
```

### 3.3 Coleta de Dados de Transparência (portal-estado-am)

**Ator:** Container portal-estado-am (agendado ou sob demanda)

```
1. Lê configurações da tabela portal_estado_am.conf (exercicio, mês, credores)
2. Abre browser Playwright (Chromium headless)
3. Navega para https://sistemas.sefaz.am.gov.br/transparencia/pagamentos/credor
4. Para cada CNPJ/CPF configurado:
   a. Preenche formulário de busca
   b. Coleta lista de pagamentos (num_ob, orgão, valor, datas)
   c. Para cada pagamento, acessa detalhes e coleta NL e empenhos
   d. Verifica duplicata antes de inserir (num_ob / num_nl)
   e. Insere em portal_estado_am.pagamentos e nl_itens
5. Registra execução em portal_estado_am.execucao_logs
```

### 3.4 ETL de Pagamentos (portal-cleaner)

**Ator:** Container portal-cleaner (batch)

```
1. Lê registros não tratados de portal_estado_am.pagamentos
2. Para cada registro:
   a. Remove prefixos desnecessários das colunas
   b. Extrai valores financeiros (valor, valor_anulado) da descrição
   c. Parseia descrição: número NL, número NF, data, período, credor, tipo retenção
   d. Insere em pagamentos_treated
3. Processa mensagens SASI (avaliações diárias):
   a. Lê sasi_messages não processadas
   b. Extrai campos do formulário
   c. Resolve datas relativas (Hoje/Ontem)
   d. Busca CPF do trabalhador em mn_mobile_users_contract
   e. Insere em mn_daily_evaluations
   f. Marca mensagem como processada
4. Registra métricas em cleaner_log
```

### 3.5 Scraping SEFAZ (socrates)

**Ator:** Container portal-socrates (agendado via cron/systemd — 2h da manhã)

```
1. Carrega configurações das tabelas conf, conf_cpfs, conf_exercicios
2. Para cada CPF/CNPJ × exercício configurado:
   a. Abre Playwright, navega para portal SEFAZ
   b. Coleta empenhos e pagamentos
   c. Verifica cache de números já existentes
   d. Insere novos registros no banco
3. Envia notificação por e-mail aos destinatários em conf_emails
```

---

## 4. Guia de Configuração

### 4.1 Variáveis de Ambiente — Portal API (`api` service)

| Variável | Descrição | Exemplo |
|---|---|---|
| `DB_HOST` | Host do PostgreSQL (interno Docker) | `supabase-db` |
| `DB_PORT` | Porta do PostgreSQL | `5432` |
| `DB_NAME` | Nome do banco | `postgres` |
| `DB_USER` | Usuário do banco | `postgres` |
| `DB_PASSWORD` | Senha do banco | _(ver .env)_ |
| `API_KEY_PORTAL_MUNICIPAL_MANAUS` | Chave de acesso — portal municipal | _(ver .env)_ |
| `API_KEY_PORTAL_ESTADO_AM` | Chave de acesso — portal estadual | _(ver .env)_ |

> As variáveis são carregadas via `.env` no `docker-compose.yml`. O arquivo `.env` **não é versionado** (está no `.gitignore`).

### 4.2 Variáveis de Ambiente — Demais Serviços

| Serviço | Variável | Descrição |
|---|---|---|
| socrates | `SUPABASE_DB_*` | Conexão direta ao PostgreSQL |
| aristoteles | `SUPABASE_DB_*` | Conexão direta ao PostgreSQL |
| aristoteles | `LOG_FILE` | Caminho do arquivo de log |
| aristoteles | `MAX_WORKERS` | Threads para processamento paralelo |
| portal-estado-am | `SUPABASE_DB_*` | Conexão direta ao PostgreSQL |

### 4.3 Configuração do ProcessadorNF (cliente Windows)

O app armazena configuração em:
```
C:\Users\{usuario}\AppData\Roaming\ProcessadorNF\conf.ini
```

Estrutura do arquivo:
```ini
[PERFIL_1]
nome     = PREFEITURA MANAUS
smb_path = \\192.168.51.200\compartilhamento\PREFEITURA_MAO
usuario  = dominio\usuario
senha    = senha_smb
portal   = portal_municipal_manaus

[PERFIL_2]
nome     = ESTADO AM
smb_path = \\192.168.51.200\compartilhamento\ESTADO_AM
usuario  = dominio\usuario
senha    = senha_smb
portal   = portal_estado_am
```

A tela de configuração é protegida por senha. Para adicionar/editar perfis, acesse **Configurações** na interface e insira a senha definida no build.

### 4.4 Endpoints da Portal API

**Base URL:** `http://187.77.240.80:9000`

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Verifica se a API está no ar |
| `POST` | `/portal-municipal-manaus/faturamento` | Insere NFS-e municipal |
| `GET` | `/portal-municipal-manaus/faturamento/existe/{numero}` | Verifica duplicata |
| `POST` | `/portal-estado-am/faturamento` | Insere NFS-e estadual |
| `GET` | `/portal-estado-am/faturamento/existe/{numero}` | Verifica duplicata |
| `GET` | `/portal-estado-am/pagamentos` | Lista pagamentos (filtros: exercicio, mes, orgao) |
| `GET` | `/portal-estado-am/nl-itens` | Lista empenhos/NL |
| `GET` | `/portal-estado-am/resumo` | Totais e métricas |
| `GET` | `/portal-estado-am/logs` | Logs de execução |
| `GET` | `/portal-estado-am/conf` | Lista configurações |
| `PUT` | `/portal-estado-am/conf/{chave}` | Atualiza configuração |

**Autenticação:** todas as rotas (exceto `/health`) exigem o header `x-api-key`.

**Documentação interativa (Swagger):** `http://187.77.240.80:9000/docs`

### 4.5 Dependências — Build do EXE (ProcessadorNF)

```bash
pip install pyinstaller pdfplumber PyPDF2 pytesseract pdf2image Pillow requests

# Gerar EXE
pyinstaller app.py --onefile --windowed --name ProcessadorNF
```

O executável gerado fica em `dist/ProcessadorNF.exe`. Não requer instalação — basta distribuir o `.exe`.

### 4.6 Deploy no VPS

```bash
# Subir todos os serviços
cd /opt/portal
docker compose up -d

# Reconstruir apenas a API após mudanças
docker compose up -d --build api

# Ver logs em tempo real
docker compose logs -f portal-api

# Status dos containers
docker compose ps
```

### 4.7 Estrutura do Repositório

```
PROJETO-SOCRATES-VPS/
├── main.py                  # Entry point FastAPI
├── auth.py                  # Validação de API keys
├── Dockerfile               # Build da portal-api
├── docker-compose.yml       # Orquestração completa
├── .gitignore
│
├── routers/
│   ├── portal_municipal_manaus.py
│   └── portal_estado_am.py
│
├── ProcessadorNF/           # Código-fonte do cliente Windows
│   ├── app.py
│   ├── extractor.py
│   ├── pdf_reader.py
│   ├── supabase_client.py
│   └── utils.py
│
├── aristoteles/             # Watcher Docker (legado)
├── cleaner/                 # ETL de pagamentos
├── socrates/                # Scraper SEFAZ (socrates)
├── portal-estado-am/        # Scraper SEFAZ (estado AM)
└── scripts/
    └── sync_procmail.py     # Sincronização de filtros de e-mail
```

---

## 5. Fluxograma

### Fluxo Principal — ProcessadorNF

```mermaid
flowchart TD
    A([Usuário abre ProcessadorNF.exe]) --> B[Seleciona perfis na interface]
    B --> C{Senha configuração?}
    C -- Editar perfis --> D[Dialog: Configurações\nAdd / Edit / Delete perfil]
    D --> B
    C -- Processar --> E[Para cada perfil selecionado]

    E --> F[Autenticar SMB\nnet use + usuário/senha]
    F --> G{Conexão OK?}
    G -- Não --> H[Exibe erro e passa para próximo perfil]
    G -- Sim --> I[Lista PDFs na pasta\nexclui /processados e /erro]

    I --> J{PDFs encontrados?}
    J -- Não --> K[Log: nenhum arquivo]
    J -- Sim --> L[Para cada PDF]

    L --> M[Extrai texto do PDF]
    M --> M1{pdfplumber OK?}
    M1 -- Sim --> N
    M1 -- Não --> M2{PyPDF2 OK?}
    M2 -- Sim --> N
    M2 -- Não --> M3[Tesseract OCR]
    M3 --> N[Texto extraído]

    N --> O[Detecta modelo:\nDANFSE / NOTA / FATURA]
    O --> P{Modelo identificado?}
    P -- Não --> Q[Move para /erro\nLog: modelo desconhecido]
    P -- Sim --> R[Extrai campos estruturados\nnúmero, data, CNPJs, valores]

    R --> S[GET /portal/faturamento/existe/{numero}]
    S --> T{Já existe?}
    T -- Sim --> U[Move para /processados\nLog: duplicata]
    T -- Não --> V[POST /portal/faturamento\nJSON com dados da nota]

    V --> W{HTTP 200?}
    W -- Sim, inserido --> X[Move para /processados\nLog: inserido]
    W -- Sim, duplicata --> U
    W -- Erro --> Y[Move para /erro\nLog: falha na API]

    X --> L
    U --> L
    Q --> L
    Y --> L

    L -- Todos processados --> Z[Exibe relatório:\nInseridos / Duplicatas / Erros]
    K --> Z
    H --> Z
    Z --> E
    E -- Todos perfis concluídos --> AA([Processamento finalizado])
```

### Fluxo de Autenticação da Portal API

```mermaid
flowchart LR
    A[Cliente HTTP] -->|POST /portal-X/faturamento\nx-api-key: abc123| B[portal-api]
    B --> C{auth.py:\nAPI_KEYS.get portal}
    C -->|Chave inválida ou ausente| D[HTTP 401 Unauthorized]
    C -->|Chave válida| E[Router executa]
    E --> F[(PostgreSQL\nsupabase-db:5432)]
    F -->|rowcount = 1| G[status: inserido]
    F -->|rowcount = 0| H[status: duplicata]
```

### Fluxo de Coleta — Portal Estado AM

```mermaid
flowchart TD
    A([Container inicia]) --> B[Lê conf de portal_estado_am.conf]
    B --> C[Abre Playwright / Chromium headless]
    C --> D[Para cada CNPJ configurado]
    D --> E[Navega para SEFAZ Transparência]
    E --> F[Preenche formulário de busca]
    F --> G[Coleta lista de pagamentos]
    G --> H[Para cada pagamento]
    H --> I{num_ob já existe\nno banco?}
    I -- Sim --> J[Ignora]
    I -- Não --> K[Acessa detalhes\nColeta NL e empenhos]
    K --> L[Insere em pagamentos\ne nl_itens]
    L --> H
    J --> H
    H -- Fim da lista --> D
    D -- Todos CNPJs --> M[Registra em execucao_logs]
    M --> N([Execução concluída])
```
