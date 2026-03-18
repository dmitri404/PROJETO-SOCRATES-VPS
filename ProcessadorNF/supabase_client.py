"""
supabase_client.py
Envia notas fiscais para a Portal API, que insere no Supabase.
"""

import requests


def _br_to_float(value):
    if not value:
        return None
    try:
        return float(str(value).replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return None


class SupabaseClient:

    _ENDPOINTS = {
        'portal_municipal_manaus': 'portal-municipal-manaus',
        'portal_estado_am':        'portal-estado-am',
    }

    def __init__(self, url: str, api_key: str, portal: str):
        self._base = url.rstrip('/')
        self._headers = {
            'x-api-key':    api_key,
            'Content-Type': 'application/json',
        }
        self._path = self._ENDPOINTS.get(portal, portal)
        self._numeros_cache: set | None = None

    def carregar_numeros_existentes(self) -> None:
        # Cache não disponível via API pública — duplicatas tratadas pelo servidor
        self._numeros_cache = set()

    def numero_ja_existe(self, numero: str) -> bool:
        if self._numeros_cache is not None and numero in self._numeros_cache:
            return True
        resp = requests.get(
            f'{self._base}/{self._path}/faturamento/existe/{numero}',
            headers=self._headers,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get('existe', False)

    def inserir_nota(self, dados: dict) -> bool:
        """Retorna True se inserida, False se duplicata."""
        numero = dados.get('NumeroNota', '')
        if self.numero_ja_existe(numero):
            return False

        payload = {
            'numero_nota':   numero,
            'data_emissao':  dados.get('DataEmissao'),
            'cnpj_emitente': dados.get('CNPJEmitente'),
            'cnpj_tomador':  dados.get('CNPJTomador'),
            'nome_tomador':  dados.get('NomeTomador'),
            'valor_total':   _br_to_float(dados.get('ValorTotal')),
            'valor_liquido': _br_to_float(dados.get('ValorLiquido')),
            'arquivo':       dados.get('Arquivo'),
        }

        resp = requests.post(
            f'{self._base}/{self._path}/faturamento',
            headers=self._headers,
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()

        status = resp.json().get('status')
        if status == 'duplicata':
            return False

        if self._numeros_cache is not None:
            self._numeros_cache.add(numero)
        return True
