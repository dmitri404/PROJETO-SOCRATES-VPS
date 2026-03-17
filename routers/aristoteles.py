import os
import psycopg2
from fastapi import APIRouter, Header
from pydantic import BaseModel
from typing import Optional
from auth import verificar_api_key


router = APIRouter(prefix="/aristoteles", tags=["Aristoteles"])


def _conectar():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "supabase-db"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=10,
    )


class NotaFiscal(BaseModel):
    numero_nota:   str
    data_emissao:  Optional[str] = None
    cnpj_emitente: Optional[str] = None
    cnpj_tomador:  Optional[str] = None
    nome_tomador:  Optional[str] = None
    valor_total:   Optional[float] = None
    valor_liquido: Optional[float] = None
    arquivo:       Optional[str] = None


@router.post("/faturamento")
def inserir_nota(nota: NotaFiscal, x_api_key: str = Header(...)):
    verificar_api_key("aristoteles", x_api_key)
    conn = _conectar()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO faturamento (numero_nota, data_emissao, cnpj_emitente, cnpj_tomador, "
        "nome_tomador, valor_total, valor_liquido, arquivo) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (numero_nota) DO NOTHING",
        (nota.numero_nota, nota.data_emissao, nota.cnpj_emitente, nota.cnpj_tomador,
         nota.nome_tomador, nota.valor_total, nota.valor_liquido, nota.arquivo)
    )
    inserido = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    status = "inserido" if inserido else "duplicata"
    return {"status": status, "numero_nota": nota.numero_nota}


@router.get("/faturamento/existe/{numero_nota}")
def nota_existe(numero_nota: str, x_api_key: str = Header(...)):
    verificar_api_key("aristoteles", x_api_key)
    conn = _conectar()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM faturamento WHERE numero_nota = %s LIMIT 1", (numero_nota,))
    existe = cur.fetchone() is not None
    cur.close()
    conn.close()
    return {"existe": existe}
