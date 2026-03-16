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
    "Chrome/122.0.0.0 Safari/537.36"
)

START_TIME = time.time()
LAST_REQUEST_TS = 0.0

TIPOS = {
    1: "carros",
    2: "motos",
    3: "caminhoes"
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
    "Referer": "https://veiculos.fipe.org.br/",
    "Origin": "https://veiculos.fipe.org.br",
    "Host": "veiculos.fipe.org.br",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
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
    elapsed_seconds = time.time() - START_TIME
    if elapsed_seconds > (MAX_EXECUTION_TIME_MINUTES * 60):
        print(f"Tempo limite de {MAX_EXECUTION_TIME_MINUTES} minuto(s) atingido. Solicitando continuação...")
        set_github_output("continue_execution", "true")
        raise SystemExit(0)


def sleep_checked(seconds: float) -> None:
    if seconds <= 0:
        return
    end_time = time.time() + seconds
    while True:
        check_time()
        remaining = end_time - time.time()
        if remaining <= 0:
            break
        time.sleep(min(remaining, 1.0))


def throttle(endpoint: str) -> None:
    global LAST_REQUEST_TS
    min_gap = MIN_GAP_SECONDS.get(endpoint, MIN_GAP_SECONDS["default"])
    now = time.time()
    elapsed = now - LAST_REQUEST_TS
    wait = min_gap - elapsed
    if wait > 0:
        sleep_checked(wait)
    LAST_REQUEST_TS = time.time()


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(1.0, float(value))
    except ValueError:
        return None


def schedule_continuation(reason: str) -> None:
    print(reason)
    set_github_output("continue_execution", "true")
    raise SystemExit(0)


def make_request(endpoint: str, data: dict[str, Any]) -> Optional[Any]:
    url = f"{API_BASE_URL}/{endpoint}"

    for attempt in range(1, MAX_RETRIES + 1):
        check_time()
        throttle(endpoint)

        try:
            response = SESSION.post(url, json=data, timeout=REQUEST_TIMEOUT)

            if response.status_code in RETRYABLE_STATUS_CODES:
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                wait_seconds = retry_after if retry_after is not None else min(180.0, (2 ** attempt) + random.uniform(0.5, 2.5))
                print(
                    f"HTTP {response.status_code} em {endpoint} "
                    f"(tentativa {attempt}/{MAX_RETRIES}). "
                    f"Aguardando {wait_seconds:.1f}s antes de tentar novamente..."
                )

                if attempt == MAX_RETRIES or (endpoint == "ConsultarValorComTodosParametros" and attempt >= 4):
                    schedule_continuation(
                        f"Rate limit persistente em {endpoint}. "
                        f"Encerrando ciclo atual para retomar no próximo workflow sem perder progresso."
                    )

                sleep_checked(wait_seconds)
                continue

            response.raise_for_status()

            try:
                return response.json()
            except ValueError:
                print(f"Resposta inválida em {endpoint}. Corpo não pôde ser convertido para JSON.")
                return None

        except requests.exceptions.RequestException as e:
            wait_seconds = min(90.0, (2 ** attempt) + random.uniform(0.5, 1.5))
            print(f"Erro na requisição {endpoint} (tentativa {attempt}/{MAX_RETRIES}): {e}")

            if attempt == MAX_RETRIES:
                return None

            sleep_checked(wait_seconds)

    return None


def get_tabela_referencia() -> tuple[Optional[int], Optional[str]]:
    data = make_request("ConsultarTabelaDeReferencia", {})
    if data:
        return data[0]["Codigo"], data[0]["Mes"]
    return None, None


def run_etl() -> None:
    print(f"Iniciando ETL... Banco de dados em: {DB_FILE}")
    init_db()
    conn = get_connection()
    c = conn.cursor()

    try:
        cod_ref, mes_ref = get_tabela_referencia()
        if not cod_ref:
            print("Falha ao obter tabela de referência.")
            set_github_output("continue_execution", "false")
            return

        print(f"Tabela de Referência: {mes_ref} (Código: {cod_ref})")

        c.execute("SELECT valor FROM controle WHERE chave='ref_atual'")
        row = c.fetchone()

        if row and row[0] != str(cod_ref):
            print("Nova referência detectada! Limpando banco temporário...")
            for table in ["marcas", "modelos", "anos", "precos"]:
                c.execute(f"DELETE FROM {table}")
            c.execute("UPDATE controle SET valor = ? WHERE chave='ref_atual'", (str(cod_ref),))
            conn.commit()
        elif not row:
            c.execute("INSERT INTO controle (chave, valor) VALUES ('ref_atual', ?)", (str(cod_ref),))
            conn.commit()

        print("Iniciando Fase de Marcas...")
        for tipo_id, tipo_nome in TIPOS.items():
            c.execute("SELECT count(*) FROM marcas WHERE tipo_veiculo = ?", (tipo_id,))
            if c.fetchone()[0] == 0:
                print(f"Baixando Marcas para {tipo_nome}...")
                payload = {"codigoTabelaReferencia": cod_ref, "codigoTipoVeiculo": tipo_id}
                marcas = make_request("ConsultarMarcas", payload)
                if marcas:
                    for m in marcas:
                        c.execute(
                            "INSERT OR IGNORE INTO marcas (id, nome, tipo_veiculo, ref_tabela) VALUES (?, ?, ?, ?)",
                            (m["Value"], m["Label"], tipo_id, cod_ref)
                        )
                    conn.commit()

        print("Iniciando Fase de Modelos...")
        c.execute("SELECT id, nome, tipo_veiculo FROM marcas")
        all_marcas = c.fetchall()

        for marca_id, marca_nome, tipo_id in all_marcas:
            c.execute("SELECT count(*) FROM modelos WHERE id_marca = ? AND tipo_veiculo = ?", (marca_id, tipo_id))
            if c.fetchone()[0] > 0:
                continue

            print(f"Baixando Modelos de {marca_nome} ({TIPOS[tipo_id]})...")
            payload = {
                "codigoTabelaReferencia": cod_ref,
                "codigoTipoVeiculo": tipo_id,
                "codigoMarca": marca_id
            }
            resp = make_request("ConsultarModelos", payload)

            if resp and "Modelos" in resp:
                for mod in resp["Modelos"]:
                    c.execute(
                        "INSERT OR IGNORE INTO modelos (id, nome, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, 'PENDENTE')",
                        (mod["Value"], mod["Label"], marca_id, tipo_id, cod_ref)
                    )
                conn.commit()

        print("Iniciando Fase de Anos...")
        while True:
            check_time()
            c.execute("SELECT id, nome, id_marca, tipo_veiculo FROM modelos WHERE status='PENDENTE' LIMIT 1")
            modelo = c.fetchone()
            if not modelo:
                break

            mod_id, mod_nome, marca_id, tipo_id = modelo
            print(f"Baixando Anos do Modelo {mod_nome}...")

            payload = {
                "codigoTabelaReferencia": cod_ref,
                "codigoTipoVeiculo": tipo_id,
                "codigoMarca": marca_id,
                "codigoModelo": mod_id
            }
            anos = make_request("ConsultarAnoModelo", payload)

            if anos is None:
                print(f"Falha temporária ao buscar anos para o modelo {mod_nome}. Mantendo como PENDENTE.")
                continue

            for ano in anos:
                c.execute(
                    "INSERT OR IGNORE INTO anos (codigo, nome, id_modelo, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, ?, 'PENDENTE')",
                    (ano["Value"], ano["Label"], mod_id, marca_id, tipo_id, cod_ref)
                )

            c.execute(
                "UPDATE modelos SET status='CONCLUIDO' WHERE id=? AND id_marca=? AND tipo_veiculo=?",
                (mod_id, marca_id, tipo_id)
            )
            conn.commit()

        print("Iniciando Fase de Preços...")
        processed_prices = 0

        while True:
            check_time()
            c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo FROM anos WHERE status='PENDENTE' LIMIT 1")
            ano_row = c.fetchone()
            if not ano_row:
                break

            ano_codigo, _, mod_id, marca_id, tipo_id = ano_row

            try:
                ano_mod, cod_comb = ano_codigo.split("-")
            except ValueError:
                c.execute(
                    "UPDATE anos SET status='ERRO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?",
                    (ano_codigo, mod_id, tipo_id)
                )
                conn.commit()
                continue

            payload = {
                "codigoTabelaReferencia": cod_ref,
                "codigoTipoVeiculo": tipo_id,
                "codigoMarca": marca_id,
                "codigoModelo": mod_id,
                "anoModelo": int(ano_mod),
                "codigoTipoCombustivel": int(cod_comb),
                "tipoConsulta": "tradicional"
            }

            preco = make_request("ConsultarValorComTodosParametros", payload)

            if preco is None:
                if processed_prices % 25 == 0:
                    print("Falha temporária em preço. Item mantido como PENDENTE para nova tentativa em outro ciclo.")
                continue

            if "CodigoFipe" in preco:
                c.execute(
                    """
                    INSERT OR IGNORE INTO precos
                    (codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo, ref_tabela)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        preco["CodigoFipe"],
                        preco["Marca"],
                        preco["Modelo"],
                        preco["AnoModelo"],
                        preco["Combustivel"],
                        preco["Valor"],
                        preco["MesReferencia"],
                        tipo_id,
                        cod_ref
                    )
                )
                c.execute(
                    "UPDATE anos SET codigo_fipe=?, ano_numerico=?, status='CONCLUIDO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?",
                    (preco["CodigoFipe"], preco["AnoModelo"], ano_codigo, mod_id, tipo_id)
                )
            else:
                print(f"Resposta sem CodigoFipe para ano {ano_codigo}. Mantendo item como PENDENTE.")
                continue

            processed_prices += 1
            if processed_prices % 100 == 0:
                print(f"Preços processados neste ciclo: {processed_prices}")
            conn.commit()

        c.execute("SELECT count(*) FROM modelos WHERE status='PENDENTE'")
        pend_mod = c.fetchone()[0]
        c.execute("SELECT count(*) FROM anos WHERE status='PENDENTE'")
        pend_anos = c.fetchone()[0]

        if pend_mod == 0 and pend_anos == 0:
            print("Coleta Concluída! Gerando arquivos finais...")
            generate_output_files(c, mes_ref, cod_ref)
            set_github_output("continue_execution", "false")
        else:
            print(f"Ciclo encerrado (pendentes: Modelos={pend_mod}, Anos={pend_anos}). Reiniciando...")
            set_github_output("continue_execution", "true")
    finally:
        conn.close()


def generate_output_files(c: sqlite3.Cursor, mes_ref: str, cod_ref: int) -> None:
    print("Gerando version.json...")
    version_data = {
        "version": f"{datetime.datetime.now().year}-{datetime.datetime.now().month:02d}",
        "fipe_reference": mes_ref.strip(),
        "table": cod_ref,
        "generated_at": datetime.datetime.now().isoformat()
    }

    with open(VERSION_FILE, "w", encoding="utf-8") as f:
        json.dump(version_data, f, indent=2, ensure_ascii=False)

    print("Gerando fipe_marcas.json.gz...")
    c.execute("SELECT id, nome, tipo_veiculo FROM marcas")
    data = [{"id": r[0], "nome": r[1], "tipo": r[2]} for r in c.fetchall()]
    with gzip.open(MARCAS_FILE, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print("Gerando fipe_modelos.json.gz...")
    c.execute("SELECT id, nome, id_marca, tipo_veiculo FROM modelos")
    data = [{"id": r[0], "nome": r[1], "marca_id": r[2], "tipo": r[3]} for r in c.fetchall()]
    with gzip.open(MODELOS_FILE, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print("Gerando fipe_anos.json.gz...")
    c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo, codigo_fipe, ano_numerico FROM anos WHERE status='CONCLUIDO'")
    data = [{"id": r[0], "nome": r[1], "modelo_id": r[2], "marca_id": r[3], "tipo": r[4], "codigo_fipe": r[5], "ano": r[6]} for r in c.fetchall()]
    with gzip.open(ANOS_FILE, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print("Gerando fipe_precos.json.gz...")
    c.execute("SELECT codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo FROM precos")
    data = [{"codigo_fipe": r[0], "marca": r[1], "modelo": r[2], "ano_modelo": r[3], "combustivel": r[4], "valor": r[5], "mes_referencia": r[6], "tipo": r[7]} for r in c.fetchall()]
    with gzip.open(PRECOS_FILE, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print("Arquivos gerados com sucesso!")


if __name__ == "__main__":
    run_etl()
