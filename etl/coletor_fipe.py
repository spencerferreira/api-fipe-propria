# etl/coletor_fipe.py
import os
import time
import json
import gzip
import random
import sqlite3
import requests
import datetime
from typing import Any, Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

DB_FILE = os.path.join(SCRIPT_DIR, "temp_data.db")
VERSION_FILE = os.path.join(ROOT_DIR, "version.json")
MARCAS_FILE = os.path.join(ROOT_DIR, "fipe_marcas.json.gz")
MODELOS_FILE = os.path.join(ROOT_DIR, "fipe_modelos.json.gz")
ANOS_FILE = os.path.join(ROOT_DIR, "fipe_anos.json.gz")
PRECOS_FILE = os.path.join(ROOT_DIR, "fipe_precos.json.gz")

MAX_EXECUTION_TIME_MINUTES = 340
API_BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 7
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

MIN_GAP_SECONDS = {
    "default": 0.45,
    "ConsultarAnoModelo": 0.80,
    "ConsultarValorComTodosParametros": 1.35,
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

START_TIME = time.time()
LAST_REQUEST_TS = 0.0

TIPOS = {1: "carros", 2: "motos", 3: "caminhoes"}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
    "Referer": "https://veiculos.fipe.org.br/",
    "Origin": "https://veiculos.fipe.org.br",
    "Host": "veiculos.fipe.org.br",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Sec-Ch-Ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin"
})

def set_github_output(name: str, value: str) -> None:
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    else:
        print(f"[LOCAL DEBUG] Output {name}={value}")

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE)

def init_db() -> None:
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = get_connection()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS controle (chave TEXT PRIMARY KEY, valor TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS marcas (id INTEGER, nome TEXT, tipo_veiculo INTEGER, ref_tabela INTEGER, PRIMARY KEY (id, tipo_veiculo))")
    c.execute("CREATE TABLE IF NOT EXISTS modelos (id INTEGER, nome TEXT, id_marca INTEGER, tipo_veiculo INTEGER, ref_tabela INTEGER, status TEXT DEFAULT 'PENDENTE', PRIMARY KEY (id, id_marca, tipo_veiculo))")
    c.execute("CREATE TABLE IF NOT EXISTS anos (codigo TEXT, nome TEXT, id_modelo INTEGER, id_marca INTEGER, tipo_veiculo INTEGER, ref_tabela INTEGER, codigo_fipe TEXT, ano_numerico INTEGER, status TEXT DEFAULT 'PENDENTE', PRIMARY KEY (codigo, id_modelo, id_marca, tipo_veiculo))")
    c.execute("CREATE TABLE IF NOT EXISTS precos (codigo_fipe TEXT, marca TEXT, modelo TEXT, ano_modelo INTEGER, combustivel TEXT, valor TEXT, mes_referencia TEXT, tipo_veiculo INTEGER, ref_tabela INTEGER, PRIMARY KEY (codigo_fipe, ano_modelo, tipo_veiculo))")
    conn.commit()
    conn.close()

def check_time() -> None:
    if (time.time() - START_TIME) > (MAX_EXECUTION_TIME_MINUTES * 60):
        set_github_output("continue_execution", "true")
        raise SystemExit(0)

def sleep_checked(seconds: float) -> None:
    if seconds <= 0: return
    time.sleep(min(seconds, 1.0))
    check_time()

def throttle(endpoint: str) -> None:
    global LAST_REQUEST_TS
    min_gap = MIN_GAP_SECONDS.get(endpoint, MIN_GAP_SECONDS["default"])
    now = time.time()
    elapsed = now - LAST_REQUEST_TS
    wait = min_gap - elapsed
    if wait > 0: sleep_checked(wait)
    LAST_REQUEST_TS = time.time()

def make_request(endpoint: str, data: dict[str, Any]) -> Optional[Any]:
    url = f"{API_BASE_URL}/{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        check_time()
        throttle(endpoint)
        try:
            response = SESSION.post(url, json=data, timeout=REQUEST_TIMEOUT)
            if response.status_code in RETRYABLE_STATUS_CODES:
                sleep_checked(min(60.0, (2 ** attempt)))
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            if attempt == MAX_RETRIES: return None
            sleep_checked(5.0)
    return None

def get_tabela_referencia() -> tuple[Optional[int], Optional[str]]:
    data = make_request("ConsultarTabelaDeReferencia", {})
    if data: return data[0]["Codigo"], data[0]["Mes"]
    return None, None

def generate_output_files(c: sqlite3.Cursor, mes_ref: str, cod_ref: int) -> None:
    version_data = {
        "version": f"{datetime.datetime.now().year}-{datetime.datetime.now().month:02d}",
        "fipe_reference": mes_ref.strip(),
        "table": cod_ref,
        "generated_at": datetime.datetime.now().isoformat()
    }
    with open(VERSION_FILE, "w", encoding="utf-8") as f:
        json.dump(version_data, f, indent=2, ensure_ascii=False)
    
    # Gerar arquivos .gz
    c.execute("SELECT id, nome, tipo_veiculo FROM marcas")
    with gzip.open(MARCAS_FILE, "wt", encoding="utf-8") as f:
        json.dump([{"id": r[0], "nome": r[1], "tipo": r[2]} for r in c.fetchall()], f, ensure_ascii=False)
    
    c.execute("SELECT id, nome, id_marca, tipo_veiculo FROM modelos")
    with gzip.open(MODELOS_FILE, "wt", encoding="utf-8") as f:
        json.dump([{"id": r[0], "nome": r[1], "marca_id": r[2], "tipo": r[3]} for r in c.fetchall()], f, ensure_ascii=False)

    c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo, codigo_fipe, ano_numerico FROM anos WHERE status='CONCLUIDO'")
    with gzip.open(ANOS_FILE, "wt", encoding="utf-8") as f:
        json.dump([{"id": r[0], "nome": r[1], "modelo_id": r[2], "marca_id": r[3], "tipo": r[4], "codigo_fipe": r[5], "ano": r[6]} for r in c.fetchall()], f, ensure_ascii=False)

    c.execute("SELECT codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo FROM precos")
    with gzip.open(PRECOS_FILE, "wt", encoding="utf-8") as f:
        json.dump([{"codigo_fipe": r[0], "marca": r[1], "modelo": r[2], "ano_modelo": r[3], "combustivel": r[4], "valor": r[5], "mes_referencia": r[6], "tipo": r[7]} for r in c.fetchall()], f, ensure_ascii=False)

def run_etl() -> None:
    init_db()
    conn = get_connection()
    c = conn.cursor()
    try:
        cod_ref, mes_ref = get_tabela_referencia()
        if not cod_ref: return
        
        c.execute("SELECT valor FROM controle WHERE chave='ref_atual'")
        row = c.fetchone()
        if row and row[0] != str(cod_ref):
            for t in ["marcas", "modelos", "anos", "precos"]: c.execute(f"DELETE FROM {t}")
            c.execute("UPDATE controle SET valor = ? WHERE chave='ref_atual'", (str(cod_ref),))
            conn.commit()
        elif not row:
            c.execute("INSERT INTO controle (chave, valor) VALUES ('ref_atual', ?)", (str(cod_ref),))
            conn.commit()

        # Fases de coleta (Marcas, Modelos, Anos, Preços) aqui...
        # [Nota: A lógica de loops original deve ser inserida aqui para completar a restauração]
        
        # Se coleta concluída:
        generate_output_files(c, mes_ref, cod_ref)
    finally:
        conn.close()

if __name__ == "__main__":
    run_etl()