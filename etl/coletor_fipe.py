import os
import time
import json
import gzip
import sqlite3
import requests
import datetime

# --- CONFIGURAÇÕES ---

# CORREÇÃO 1: Caminho absoluto para o DB para evitar erros de diretório
# O DB será salvo na mesma pasta do script (etl/temp_data.db)
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_data.db")

# CORREÇÃO 2 (TEMPORÁRIA): Reduzir o tempo para um teste rápido
MAX_EXECUTION_TIME_MINUTES = 2 # Teste rápido para verificar a criação do .db

API_BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
START_TIME = time.time()

# Tipos de Veículos
TIPOS = {
    1: "carros",
    2: "motos",
    3: "caminhoes"
}

# --- LÓGICA DO SCRIPT ---

def set_github_output(name, value):
    """Escreve uma variável de saída para o GitHub Actions ler."""
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"{name}={value}\n")
    else:
        print(f"[LOCAL DEBUG] Output {name}={value}")

def get_connection():
    return sqlite3.connect(DB_FILE)

def init_db():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = get_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS controle (chave TEXT PRIMARY KEY, valor TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS marcas (id INTEGER, nome TEXT, tipo_veiculo INTEGER, ref_tabela INTEGER, PRIMARY KEY (id, tipo_veiculo))''')
    c.execute('''CREATE TABLE IF NOT EXISTS modelos (id INTEGER, nome TEXT, id_marca INTEGER, tipo_veiculo INTEGER, ref_tabela INTEGER, status TEXT DEFAULT 'PENDENTE', PRIMARY KEY (id, id_marca, tipo_veiculo))''')
    c.execute('''CREATE TABLE IF NOT EXISTS anos (codigo TEXT, nome TEXT, id_modelo INTEGER, id_marca INTEGER, tipo_veiculo INTEGER, ref_tabela INTEGER, codigo_fipe TEXT, ano_numerico INTEGER, status TEXT DEFAULT 'PENDENTE', PRIMARY KEY (codigo, id_modelo, id_marca, tipo_veiculo))''')
    c.execute('''CREATE TABLE IF NOT EXISTS precos (codigo_fipe TEXT, marca TEXT, modelo TEXT, ano_modelo INTEGER, combustivel TEXT, valor TEXT, mes_referencia TEXT, tipo_veiculo INTEGER, ref_tabela INTEGER, PRIMARY KEY (codigo_fipe, ano_modelo, tipo_veiculo))''')
    conn.commit()
    conn.close()

def check_time():
    if (time.time() - START_TIME) > (MAX_EXECUTION_TIME_MINUTES * 60):
        print(f"Tempo limite de {MAX_EXECUTION_TIME_MINUTES} minuto(s) atingido. Solicitando continuação...")
        set_github_output("continue_execution", "true")
        exit(0)

def make_request(endpoint, data):
    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Referer": "https://veiculos.fipe.org.br/",
        "Host": "veiculos.fipe.org.br"
    }
    for attempt in range(5):
        try:
            check_time()
            response = requests.post(f"{API_BASE_URL}/{endpoint}", json=data, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erro na requisição {endpoint} (Tentativa {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return None

def get_tabela_referencia():
    data = make_request("ConsultarTabelaDeReferencia", {})
    if data:
        return data[0]['Codigo'], data[0]['Mes']
    return None, None

def run_etl():
    print(f"Iniciando ETL... Banco de dados em: {DB_FILE}")
    init_db()
    conn = get_connection()
    c = conn.cursor()

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
        for table in ['marcas', 'modelos', 'anos', 'precos']: c.execute(f"DELETE FROM {table}")
        c.execute("UPDATE controle SET valor = ? WHERE chave='ref_atual'", (str(cod_ref),))
        conn.commit()
    elif not row:
         c.execute("INSERT INTO controle (chave, valor) VALUES ('ref_atual', ?)", (str(cod_ref),))
         conn.commit()

    # Fase 2: Marcas
    print("Iniciando Fase de Marcas...")
    for tipo_id, tipo_nome in TIPOS.items():
        c.execute("SELECT count(*) FROM marcas WHERE tipo_veiculo = ?", (tipo_id,))
        if c.fetchone()[0] == 0:
            print(f"Baixando Marcas para {tipo_nome}...")
            payload = {"codigoTabelaReferencia": cod_ref, "codigoTipoVeiculo": tipo_id}
            marcas = make_request("ConsultarMarcas", payload)
            if marcas:
                for m in marcas:
                    c.execute("INSERT OR IGNORE INTO marcas (id, nome, tipo_veiculo, ref_tabela) VALUES (?, ?, ?, ?)", (m['Value'], m['Label'], tipo_id, cod_ref))
                conn.commit()

    # Fase 3: Modelos
    print("Iniciando Fase de Modelos...")
    c.execute("SELECT id, nome, tipo_veiculo FROM marcas")
    all_marcas = c.fetchall()
    for marca_id, marca_nome, tipo_id in all_marcas:
        c.execute("SELECT count(*) FROM modelos WHERE id_marca = ? AND tipo_veiculo = ?", (marca_id, tipo_id))
        if c.fetchone()[0] > 0:
            continue
        print(f"Baixando Modelos de {marca_nome} ({TIPOS[tipo_id]})...")
        payload = {"codigoTabelaReferencia": cod_ref, "codigoTipoVeiculo": tipo_id, "codigoMarca": marca_id}
        resp = make_request("ConsultarModelos", payload)
        if resp and 'Modelos' in resp:
            for mod in resp['Modelos']:
                c.execute("INSERT OR IGNORE INTO modelos (id, nome, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, 'PENDENTE')", (mod['Value'], mod['Label'], marca_id, tipo_id, cod_ref))
            conn.commit()

    # Fase 4: Anos
    print("Iniciando Fase de Anos...")
    while True:
        check_time()
        c.execute("SELECT id, nome, id_marca, tipo_veiculo FROM modelos WHERE status='PENDENTE' LIMIT 1")
        modelo = c.fetchone()
        if not modelo: break
        mod_id, mod_nome, marca_id, tipo_id = modelo
        print(f"Baixando Anos do Modelo {mod_nome}...")
        payload = {"codigoTabelaReferencia": cod_ref, "codigoTipoVeiculo": tipo_id, "codigoMarca": marca_id, "codigoModelo": mod_id}
        anos = make_request("ConsultarAnoModelo", payload)
        if anos:
            for ano in anos:
                c.execute("INSERT OR IGNORE INTO anos (codigo, nome, id_modelo, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, ?, 'PENDENTE')", (ano['Value'], ano['Label'], mod_id, marca_id, tipo_id, cod_ref))
        c.execute("UPDATE modelos SET status='CONCLUIDO' WHERE id=? AND id_marca=? AND tipo_veiculo=?", (mod_id, marca_id, tipo_id))
        conn.commit()

    # Fase 5: Preços
    print("Iniciando Fase de Preços...")
    while True:
        check_time()
        c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo FROM anos WHERE status='PENDENTE' LIMIT 1")
        ano_row = c.fetchone()
        if not ano_row: break
        ano_codigo, _, mod_id, marca_id, tipo_id = ano_row
        try:
            ano_mod, cod_comb = ano_codigo.split('-')
        except:
            c.execute("UPDATE anos SET status='ERRO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?", (ano_codigo, mod_id, tipo_id))
            conn.commit()
            continue
        payload = {"codigoTabelaReferencia": cod_ref, "codigoTipoVeiculo": tipo_id, "codigoMarca": marca_id, "codigoModelo": mod_id, "anoModelo": int(ano_mod), "codigoTipoCombustivel": int(cod_comb), "tipoConsulta": "tradicional"}
        preco = make_request("ConsultarValorComTodosParametros", payload)
        if preco and 'CodigoFipe' in preco:
            c.execute('''INSERT OR IGNORE INTO precos (codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo, ref_tabela) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (preco['CodigoFipe'], preco['Marca'], preco['Modelo'], preco['AnoModelo'], preco['Combustivel'], preco['Valor'], preco['MesReferencia'], tipo_id, cod_ref))
            c.execute("UPDATE anos SET codigo_fipe=?, ano_numerico=?, status='CONCLUIDO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?", (preco['CodigoFipe'], preco['AnoModelo'], ano_codigo, mod_id, tipo_id))
        else:
            c.execute("UPDATE anos SET status='ERRO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?", (ano_codigo, mod_id, tipo_id))
        conn.commit()

    # Fase 6: Finalização
    c.execute("SELECT count(*) FROM modelos WHERE status='PENDENTE'")
    pend_mod = c.fetchone()[0]
    c.execute("SELECT count(*) FROM anos WHERE status='PENDENTE'")
    pend_anos = c.fetchone()[0]
    if pend_mod == 0 and pend_anos == 0:
        print("Coleta Concluída! Gerando arquivos finais...")
        generate_output_files(c, mes_ref)
        set_github_output("continue_execution", "false")
    else:
        print(f"Ciclo encerrado (pendentes: Modelos={pend_mod}, Anos={pend_anos}). Reiniciando...")
        set_github_output("continue_execution", "true")

    conn.close()

def generate_output_files(c, mes_ref):
    print("Gerando version.json...")
    version_data = {"version": f"{datetime.datetime.now().year}-{datetime.datetime.now().month:02d}", "fipe_reference": mes_ref, "generated_at": datetime.datetime.now().isoformat()}
    with open("version.json", "w", encoding="utf-8") as f:
        json.dump(version_data, f, indent=2)
    
    # Exportar tabelas
    print("Gerando fipe_marcas.json.gz...")
    c.execute("SELECT id, nome, tipo_veiculo FROM marcas")
    data = [{"id": r[0], "nome": r[1], "tipo": r[2]} for r in c.fetchall()]
    with gzip.open("fipe_marcas.json.gz", "wt", encoding="utf-8") as f: json.dump(data, f)
        
    print("Gerando fipe_modelos.json.gz...")
    c.execute("SELECT id, nome, id_marca, tipo_veiculo FROM modelos")
    data = [{"id": r[0], "nome": r[1], "marca_id": r[2], "tipo": r[3]} for r in c.fetchall()]
    with gzip.open("fipe_modelos.json.gz", "wt", encoding="utf-8") as f: json.dump(data, f)
        
    print("Gerando fipe_anos.json.gz...")
    c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo, codigo_fipe, ano_numerico FROM anos WHERE status='CONCLUIDO'")
    data = [{"id": r[0], "nome": r[1], "modelo_id": r[2], "marca_id": r[3], "tipo": r[4], "codigo_fipe": r[5], "ano": r[6]} for r in c.fetchall()]
    with gzip.open("fipe_anos.json.gz", "wt", encoding="utf-8") as f: json.dump(data, f)
        
    print("Gerando fipe_precos.json.gz...")
    c.execute("SELECT codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo FROM precos")
    data = [{"codigo_fipe": r[0], "marca": r[1], "modelo": r[2], "ano_modelo": r[3], "combustivel": r[4], "valor": r[5], "mes_referencia": r[6], "tipo": r[7]} for r in c.fetchall()]
    with gzip.open("fipe_precos.json.gz", "wt", encoding="utf-8") as f: json.dump(data, f)
        
    print("Arquivos gerados com sucesso!")

if __name__ == "__main__":
    run_etl()
