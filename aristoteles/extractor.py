"""
extractor.py — Detecção do modelo de nota fiscal e extração de campos.

Modelos suportados:
    FATURA  → Fatura de Locação (campo "FATURA DE LOCAÇÃO", "DADOS DO SACADO")
    DANFSE  → NFS-e DANFSe Prefeitura de Manaus ("DANFSe", "Número da NFS-e")
    NOTA    → Nota Fiscal Prefeitura de Manaus ("Número da Nota", "Data/Hora da emissão")
"""

import re
import logging
from typing import Optional

from utils import extrair_cnpjs, limpar_valor, formatar_valor_br

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tipo do documento
# ---------------------------------------------------------------------------

def detect_pdf_type(texto: str) -> str:
    """
    Analisa o texto extraído e identifica o modelo da nota fiscal.

    Retorna uma das strings: 'FATURA' | 'DANFSE' | 'NOTA' | 'DESCONHECIDO'
    """
    upper = texto.upper()

    # Modelo 2 — NFS-e DANFSe (verificar antes de NOTA pois ambos são da Prefeitura)
    if any(kw in upper for kw in ("DANFSE", "NÚMERO DA NFS-E", "NUMERO DA NFS-E",
                                   "COMPETÊNCIA DA NFS-E", "COMPETENCIA DA NFS-E",
                                   "VALOR LÍQUIDO DA NFS-E", "VALOR LIQUIDO DA NFS-E")):
        return "DANFSE"

    # Modelo 3 — Número da Nota (Prefeitura de Manaus)
    # "MERO DA NOTA" cobre NÚMERO/NUMERO e variações com bytes CP1252 (N\x82MERO)
    if "MERO DA NOTA" in upper and any(kw in upper for kw in (
        "DATA/HORA DA EMISS",   # cobre EMISSÃO/EMISSAO/EMISS\x83O
        "QUIDO DA NOTA",        # cobre LÍQUIDO/LIQUIDO/L\x82QUIDO
        "VALOR TOTAL DA NOTA = R$",
    )):
        return "NOTA"

    # Modelo 1 — Fatura de Locação
    # "DADOS DO SACADO" e "VALOR TOTAL DA NOTA" não têm acentos — match direto
    if any(kw in upper for kw in ("DADOS DO SACADO", "VALOR TOTAL DA NOTA",
                                   "FATURA DE LOCA", "FATURA/DUPLICATA")):
        return "FATURA"

    # Tentativa genérica: "NOTA FISCAL" com campos básicos
    if "NOTA FISCAL" in upper and re.search(r'N[ÚU]MERO|N[Oº°]\s*\d', texto, re.IGNORECASE):
        return "FATURA"

    return "DESCONHECIDO"


# ---------------------------------------------------------------------------
# Extratores — MODELO 1: Fatura de Locação
# ---------------------------------------------------------------------------

def _numero_fatura(texto: str) -> str:
    """
    Extrai número da Fatura de Locação.

    Texto real: "No. Fatura/Duplicata: ...\nN\x82 380 06/02/2026 ..."
    pdfplumber codifica 'º' como '\x82' (CP1252), então usamos '.' (qualquer char).
    O padrão mais confiável: N + qualquer char + espaço + número + espaço + data.
    """
    patterns = [
        # Layout pós-cabeçalho: linha seguinte contém [Nº] número data
        # Cobre tanto "Nº 345 01/12/2025" quanto "344 01/12/2025" (sem prefixo)
        r'Fatura/Duplicata[^\n]*\n\s*(?:N.\s+)?(\d{1,8})\s+\d{2}/\d{2}/\d{4}',
        # Fallback explícito sem acento
        r'N[Oo]\.\s*(\d{1,8})\b',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _data_fatura(texto: str) -> str:
    """
    Extrai data de emissão da Fatura.
    Formato esperado: DD/MM/AAAA
    """
    patterns = [
        r'Data\s+de\s+[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'Data\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'(\d{2}/\d{2}/\d{4})',   # qualquer data — último recurso
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _valor_fatura(texto: str) -> str:
    """
    Extrai valor líquido da Fatura.
    Prioridade: "TOTAL LÍQUIDO A RECEBER" > "VALOR TOTAL DA NOTA"
    Texto real: "TOTAL LÍQUIDO A RECEBER R$ 10.727,08"
    Nota: pdfplumber pode inserir espaços dentro do número (ex: "3 .701,62").
    """
    patterns = [
        # Valor líquido (após descontos) — prioridade máxima
        # Usa '.' para 'Í' pois pdfplumber pode retornar \x84 (CP1252) no lugar de Í
        r'TOTAL\s+L.QUIDO\s+A\s+RECEBER\s+R\$\s*([\d., ]+)',
        r'VALOR\s+L.QUIDO\s+A\s+RECEBER\s+R\$\s*([\d., ]+)',
        # Valor bruto — fallback (sem acentos → match direto)
        r'VALOR\s+TOTAL\s+DA\s+NOTA\s*=?\s*R\$\s*([\d., ]+)',
        r'VALOR\s+TOTAL\s+DA\s+NOTA\s+R\$\s*([\d., ]+)',
        r'TOTAL\s+DA\s+NOTA\s*[:\-]?\s*R?\$?\s*([\d., ]+)',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _valor_total_fatura(texto: str) -> str:
    """
    Extrai valor total (bruto) da Fatura — campo "VALOR TOTAL DA NOTA".
    Nota: pdfplumber pode inserir espaços dentro do número (ex: "3 .701,62").
    """
    patterns = [
        r'VALOR\s+TOTAL\s+DA\s+NOTA\s*=?\s*R\$\s*([\d., ]+)',
        r'VALOR\s+TOTAL\s+DA\s+NOTA\s+R\$\s*([\d., ]+)',
        r'TOTAL\s+DA\s+NOTA\s*[:\-]?\s*R?\$?\s*([\d., ]+)',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _tomador_fatura(texto: str) -> str:
    """
    Extrai nome do tomador/sacado da Fatura.
    Texto real: "Nome: SECRETARIA... CNPJ: XX.XXX.XXX/XXXX-XX"
    (nome e CNPJ na mesma linha — extrai só o nome)
    """
    # Layout real: "Nome: SECRETARIA DE ESTADO... CNPJ: XX.XXX..."
    # Captura tudo entre "Nome:" e " CNPJ:" (sem usar |$ que causa match prematuro)
    m = re.search(r'Nome:\s+(.+?)\s+CNPJ\s*:', texto, re.IGNORECASE)
    if m:
        nome = m.group(1).strip()
        if len(nome) > 3:
            return nome

    # Layouts alternativos
    for pat in [
        r'DADOS\s+DO\s+SACADO[\s\S]{0,10}\n([^\n\r]{3,80})',
        r'SACADO\s*[:\-]?\s*([^\n\r]{3,80})',
        r'CONTRATANTE\s*[:\-]?\s*([^\n\r]{3,80})',
    ]:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            nome = m.group(1).strip()
            if not re.search(r'\d{2}[./]\d{3}', nome) and len(nome) > 3:
                return nome
    return ""


# ---------------------------------------------------------------------------
# Extratores — MODELO 2: NFS-e DANFSe
# ---------------------------------------------------------------------------

def _numero_danfse(texto: str) -> str:
    """
    Extrai número da NFS-e.

    Layout normal:  "Número da NFS-e  171"
    Layout compacto (sem espaços): "NúmerodaNFS-e CompetênciadaNFS-e DataeHoradaemissãodaNFS-e\n171 11/02/2026"
    → número fica na PRÓXIMA LINHA após o cabeçalho; é o único campo de 1-6 dígitos
      seguido de espaço antes de uma data DD/MM/AAAA.
    """
    patterns = [
        # Layout normal (com espaços entre palavras)
        r'N[úu]mero\s+da\s+NFS-e\s*[:\-]?\s*(\d+)',
        r'NFS-e\s*[:\-]?\s*N[oº°\.]\s*(\d+)',
        # Layout compacto: último "NFS-e" do cabeçalho → quebra de linha → número → espaço → data
        r'NFS-e\s*\n\s*(\d{1,6})\s+\d{2}/',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _data_danfse(texto: str) -> str:
    """
    Extrai data de emissão da NFS-e.

    Layout compacto: "NúmerodaNFS-e CompetênciadaNFS-e DataeHoradaemissãodaNFS-e\n171 11/02/2026 11/02/202610:17:25"
    → data de emissão = SEGUNDA data na linha de dados (após o número e a competência).
    """
    # Layout compacto: linha de dados após cabeçalho "NFS-e"
    # Formato: <numero> <competencia DD/MM/AAAA> <emissao DD/MM/AAAAhh:mm:ss>
    m = re.search(
        r'NFS-e\s*\n\s*\d{1,6}\s+\d{2}/\d{2}/\d{4}\s+(\d{2}/\d{2}/\d{4})',
        texto, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()

    # Layout normal
    patterns = [
        r'Data\s+[Ee]Hora\s+da\s+[Ee]miss[aã]o\s+da\s+NFS-e\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'Data\s+de\s+[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'Compet[êe]ncia\s+da\s+NFS-e\s*[:\-]?\s*(\d{2}/\d{4})',
        r'(\d{2}/\d{2}/\d{4})',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _valor_danfse(texto: str) -> str:
    """
    Extrai valor líquido da NFS-e.

    Layout normal:   "Valor Líquido da NFS-e  R$ 1.500,00"
    Layout compacto: "TotaldasRetençõesFederais PIS/COFINS ValorLíquidodaNFS-e\n- R$4,67 R$128,00"
    → valor líquido = ÚLTIMO R$ na linha de dados.
    """
    # Layout compacto: "ValorL...daNFS-e" como último cabeçalho da linha
    m = re.search(r'ValorL.quidodaNFS-e\s*\n([^\n]+)', texto, re.IGNORECASE)
    if m:
        valores = re.findall(r'R\$([\d.,]+)', m.group(1))
        if valores:
            return valores[-1]  # último valor da linha = valor líquido

    # Layout normal
    patterns = [
        r'Valor\s+L[íi]quido\s+da\s+NFS-e\s*[:\-]?\s*R?\$?\s*([\d.,]+)',
        r'Valor\s+L[íi]quido\s*[:\-]?\s*R?\$?\s*([\d.,]+)',
        r'VALOR\s+L[ÍI]QUIDO\s*[:\-]?\s*R?\$?\s*([\d.,]+)',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _valor_total_danfse(texto: str) -> str:
    """
    Extrai valor total (bruto) da NFS-e — campo "Valor do Serviço".

    Layout compacto (pdfplumber):
        "VALORTOTALDA NFS-E\nValordoServi.o ...(outras colunas)...\nR$128,00 ..."
    → cabeçalho de seção → linha com rótulos colados → linha de valores (primeiro = total).

    Layout normal:
        "Valor do Serviço\nR$ 128,00"
    """
    # Layout compacto: âncora na seção + rótulo colado + primeiro R$ da linha de valores
    m = re.search(
        r'VALORTOTALDA\s*NFS.E\s*\nValordoServi.o[^\n]*\nR\$([\d.,]+)',
        texto, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()

    # Layout normal: rótulo com espaços + valor na linha seguinte
    m = re.search(r'Valor\s+do\s+Servi.o\s*\n\s*R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Fallback: valor na mesma linha
    m = re.search(r'Valor\s+do\s+Servi.o\s*[:\-]?\s*R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return ""


def _tomador_danfse(texto: str) -> str:
    """
    Extrai nome do tomador de serviços da NFS-e.

    Layout compacto: "TOMADORDOSERVIÇO CNPJ/CPF/NIF ...\n03.772.576/0019-94 ...\nNome/NomeEmpresarial E-mail\nSENAI-SERVICONACIONAL..."
    Layout normal:   "Tomador de Serviços\nEmpresa ABC S/A"
    """
    # Layout compacto: seção TOMADOR → linha de CNPJ → "Nome/NomeEmpresarial" → nome
    m = re.search(
        r'TOMADOR\S+SERVI\S+[^\n]*\n[^\n]+\nNome/NomeEmpresarial[^\n]*\n([^\n]+)',
        texto, re.IGNORECASE
    )
    if m:
        nome = re.sub(r'\s*-\s*$', '', m.group(1)).strip()  # remove " -" final
        if len(nome) > 3:
            return nome

    # Layout normal
    for pat in [
        r'Tomador\s+de\s+Servi[çc]os?\s*\n([^\n\r]{3,80})',
        r'Tomador\s*[:\-]?\s*([^\n\r]{3,80})',
        r'TOMADOR\s*[:\-]?\s*([^\n\r]{3,80})',
    ]:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            nome = m.group(1).strip()
            if not re.search(r'\d{2}[./]\d{3}', nome) and len(nome) > 3:
                return nome
    return ""


# ---------------------------------------------------------------------------
# Extratores — MODELO 3: Número da Nota (Prefeitura de Manaus)
# ---------------------------------------------------------------------------

def _numero_nota_manaus(texto: str) -> str:
    """
    Extrai número da Nota da Prefeitura de Manaus.

    Layout real (colunar): "Natureza da operação  Número da Nota\n...Retenção do\n7967\nverificação."
    → número fica em linha própria, algumas linhas abaixo da label.
    """
    # Layout colunar tipo 1: número em linha isolada após "Número da Nota"
    # Texto real: "...Retenção do\n7967\nverificação."
    m = re.search(
        r'N.mero\s+da\s+Nota[\s\S]{0,250}?\n(\d{3,8})\n',
        texto, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()

    # Layout colunar tipo 2: número ao final de "ISSQN a Recolher NNN"
    # Texto real: "ISSQN a Recolher 286\nverificação."
    m = re.search(r'ISSQN\s+a\s+[Rr]ecolher\s+(\d{1,8})', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Layout direto: "Número da Nota  7967"
    m = re.search(r'N.mero\s+da\s+Nota\s*[:\-]?\s*(\d+)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return ""


def _data_nota_manaus(texto: str) -> str:
    """
    Extrai data/hora de emissão da Nota (Prefeitura de Manaus).

    Layout real (colunar): "... Data/Hora da emissão\n53CE.7321.B352 02/06/2025 - 09:20:51"
    → data está na próxima linha, após um código de verificação.
    """
    # Layout colunar: data na linha seguinte ao cabeçalho, após código alfanumérico
    m = re.search(
        r'Data/Hora\s+da\s+emiss.o\s*\n[^\n]*?(\d{2}/\d{2}/\d{4})',
        texto, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()

    # Layout direto
    patterns = [
        r'Data[/\s]+Hora\s+da\s+[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'Data\s+da\s+[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'[Ee]miss[aã]o\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})',
        r'(\d{2}/\d{2}/\d{4})',
    ]
    for pat in patterns:
        m = re.search(pat, texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _valor_nota_manaus(texto: str) -> str:
    """
    Extrai valor líquido da Nota (Prefeitura de Manaus).

    Layout real (tabular): "ISSQN(R$) Outras Deduções(R$) Total das Retenções(R$) Valor Líquido da Nota(R$)\n83,13 0,00 162,93 1.499,62"
    → valor líquido = ÚLTIMO número na linha de dados após o cabeçalho.
    """
    # Layout tabular: "Valor Líquido da Nota(R$)" é a última coluna do cabeçalho;
    # os valores ficam na linha seguinte — o último é o valor líquido.
    # Pattern simples: captura tudo até o fim da linha do cabeçalho, depois a linha de dados.
    m = re.search(r'Valor\s+L.quido\s+da\s+Nota[^\n]+\n([^\n]+)', texto, re.IGNORECASE)
    if m:
        numeros = re.findall(r'[\d.,]+', m.group(1))
        if numeros:
            return numeros[-1]  # último campo = valor líquido

    # Layout direto: valor na mesma linha após a label (sem dado tabular)
    m = re.search(r'Valor\s+L.quido\s+da\s+Nota[^\n]*?(\d[\d.,]+)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def _valor_total_nota_manaus(texto: str) -> str:
    """
    Extrai valor total da Nota (Prefeitura de Manaus) — campo "Total(R$)".

    Layout tabular: "Total(R$)" é cabeçalho de coluna; o valor fica na linha seguinte.
    """
    # Layout tabular: "Total(R$)" como cabeçalho → valor na linha seguinte
    m = re.search(r'Total\s*\(R\$\)[^\n]*\n([^\n]+)', texto, re.IGNORECASE)
    if m:
        numeros = re.findall(r'[\d.,]+', m.group(1))
        if numeros:
            return numeros[0]  # primeiro número = total bruto

    # Fallback: "VALOR TOTAL DA NOTA = R$" (caso exista em alguma variante)
    m = re.search(r'VALOR\s+TOTAL\s+DA\s+NOTA\s*=\s*R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def _tomador_nota_manaus(texto: str) -> str:
    """
    Extrai nome do tomador da Nota (Prefeitura de Manaus).

    Layout real: "Tomador de Serviço\nSECRETARIA DE ESTADO..., CIENCIA,\nNome do tomador do serviço\nTECNOLOGIA E INOVACAO"
    → nome está na linha após "Tomador de Serviço".
    """
    # Layout: nome na mesma linha que "Nome do tomador do serviço NOME..."
    m = re.search(r'Nome\s+do\s+tomador\s+do\s+servi\S*\s+([^\n]{5,})', texto, re.IGNORECASE)
    if m:
        nome = m.group(1).strip()
        if not re.search(r'\d{2}[./]\d{3}', nome) and len(nome) > 3:
            return nome

    # Layout: nome na linha imediatamente após "Tomador de Serviço"
    m = re.search(r'Tomador\s+de\s+Servi.+\n([^\n]{5,})', texto, re.IGNORECASE)
    if m:
        nome = m.group(1).strip()
        if not re.search(r'\d{2}[./]\d{3}', nome) and len(nome) > 3:
            return nome

    # Fallback: "Nome do tomador do serviço\nNOME"
    m = re.search(r'Nome\s+do\s+tomador\s+do\s+servi.+\n([^\n]{5,})', texto, re.IGNORECASE)
    if m:
        nome = m.group(1).strip()
        if not re.search(r'\d{2}[./]\d{3}', nome) and len(nome) > 3:
            return nome

    return ""


# ---------------------------------------------------------------------------
# Extração genérica (fallback)
# ---------------------------------------------------------------------------

def _extrair_generico(texto: str) -> tuple[str, str, str, str, str]:
    """
    Tentativa de extração genérica quando o tipo não é identificado.
    Retorna (numero, data, valor, valor_total, tomador).
    """
    numero = _numero_fatura(texto) or _numero_danfse(texto) or _numero_nota_manaus(texto)
    data = _data_fatura(texto)
    valor = _valor_fatura(texto) or _valor_danfse(texto) or _valor_nota_manaus(texto)
    valor_total = (
        _valor_total_fatura(texto)
        or _valor_total_danfse(texto)
        or _valor_total_nota_manaus(texto)
    )
    tomador = ""
    return numero, data, valor, valor_total, tomador


# ---------------------------------------------------------------------------
# Função pública principal
# ---------------------------------------------------------------------------

def extrair_dados(texto: str, nome_arquivo: str) -> Optional[dict]:
    """
    Identifica o tipo de nota fiscal e extrai os campos estruturados.

    Retorna dict com as chaves:
        NumeroNota, DataEmissao, CNPJEmitente, CNPJTomador,
        NomeTomador, ValorLiquido, Arquivo

    Retorna None se o número da nota não puder ser extraído.
    """
    tipo = detect_pdf_type(texto)
    logger.info("Tipo detectado: %-10s — %s", tipo, nome_arquivo)

    # Despacha para o extrator correto
    if tipo == "FATURA":
        numero      = _numero_fatura(texto)
        data        = _data_fatura(texto)
        valor       = _valor_fatura(texto)
        valor_total = _valor_total_fatura(texto)
        tomador     = _tomador_fatura(texto)

    elif tipo == "DANFSE":
        numero      = _numero_danfse(texto)
        data        = _data_danfse(texto)
        valor       = _valor_danfse(texto)
        valor_total = _valor_total_danfse(texto)
        tomador     = _tomador_danfse(texto)

    elif tipo == "NOTA":
        numero      = _numero_nota_manaus(texto)
        data        = _data_nota_manaus(texto)
        valor       = _valor_nota_manaus(texto)
        valor_total = _valor_total_nota_manaus(texto)
        tomador     = _tomador_nota_manaus(texto)

    else:
        logger.warning("Tipo desconhecido, aplicando extração genérica em '%s'", nome_arquivo)
        numero, data, valor, valor_total, tomador = _extrair_generico(texto)

    # Validação do campo obrigatório
    if not numero:
        logger.error("Número da nota não encontrado em '%s'", nome_arquivo)
        return None

    # CNPJs (primeiro = emitente, segundo = tomador por convenção)
    cnpjs = extrair_cnpjs(texto)
    cnpj_emitente = cnpjs[0] if len(cnpjs) > 0 else ""
    cnpj_tomador  = cnpjs[1] if len(cnpjs) > 1 else ""

    # Normaliza os valores para float e de volta para string BR
    valor_float       = limpar_valor(valor)
    valor_fmt         = formatar_valor_br(valor_float) if valor_float is not None else valor
    valor_total_float = limpar_valor(valor_total)
    valor_total_fmt   = formatar_valor_br(valor_total_float) if valor_total_float is not None else valor_total

    resultado = {
        "NumeroNota":   numero,
        "DataEmissao":  data,
        "CNPJEmitente": cnpj_emitente,
        "CNPJTomador":  cnpj_tomador,
        "NomeTomador":  tomador,
        "ValorLiquido": valor_fmt,
        "ValorTotal":   valor_total_fmt,
        "Arquivo":      nome_arquivo,
    }

    logger.debug("Dados extraídos: %s", resultado)
    return resultado
