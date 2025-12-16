import os
import time
import json
import gzip
import sqlite3
import requests
import datetime

# Configurações
DB_FILE = "temp_data.db"
API_BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
MAX_EXECUTION_TIME_MINUTES = 340 # ~5h40m (limite de 6h do GitHub)
START_TIME = time.time()

# Tipos de Veículos
TIPOS = {
    1: "carros",
    2: "motos",
    3: "caminhoes"
}

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
    conn = get_connection()
    c = conn.cursor()
    
    # Tabela de Controle (Estado Geral)
    c.execute('''CREATE TABLE IF NOT EXISTS controle (
        chave TEXT PRIMARY KEY,
        valor TEXT
    )''')
    
    # Tabelas de Dados
    c.execute('''CREATE TABLE IF NOT EXISTS marcas (
        id INTEGER,
        nome TEXT,
        tipo_veiculo INTEGER,
        ref_tabela INTEGER,
        PRIMARY KEY (id, tipo_veiculo)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS modelos (
        id INTEGER,
        nome TEXT,
        id_marca INTEGER,
        tipo_veiculo INTEGER,
        ref_tabela INTEGER,
        status TEXT DEFAULT 'PENDENTE', -- PENDENTE, CONCLUIDO
        PRIMARY KEY (id, id_marca, tipo_veiculo)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS anos (
        codigo TEXT, -- Ex: "2014-1"
        nome TEXT,
        id_modelo INTEGER,
        id_marca INTEGER,
        tipo_veiculo INTEGER,
        ref_tabela INTEGER,
        codigo_fipe TEXT, -- Preenchido ao buscar o preço
        ano_numerico INTEGER, -- Preenchido ao buscar o preço ou parsear
        status TEXT DEFAULT 'PENDENTE',
        PRIMARY KEY (codigo, id_modelo, id_marca, tipo_veiculo)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS precos (
        codigo_fipe TEXT,
        marca TEXT,
        modelo TEXT,
        ano_modelo INTEGER,
        combustivel TEXT,
        valor TEXT,
        mes_referencia TEXT,
        tipo_veiculo INTEGER,
        ref_tabela INTEGER,
        PRIMARY KEY (codigo_fipe, ano_modelo, tipo_veiculo)
    )''')
    
    conn.commit()
    conn.close()

def check_time():
    if (time.time() - START_TIME) > (MAX_EXECUTION_TIME_MINUTES * 60):
        print("Tempo limite atingido. Solicitando continuação...")
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
            time.sleep(2 ** attempt) # Backoff
    return None

def get_tabela_referencia():
    data = make_request("ConsultarTabelaDeReferencia", {})
    if data:
        return data[0]['Codigo'], data[0]['Mes']
    return None, None

def run_etl():
    print("Iniciando ETL...")
    init_db()
    conn = get_connection()
    c = conn.cursor()

    # 1. Obter Referência Atual
    cod_ref, mes_ref = get_tabela_referencia()
    if not cod_ref:
        print("Falha ao obter tabela de referência.")
        return

    print(f"Tabela de Referência: {mes_ref} (Código: {cod_ref})")
    
    # Verificar se mudou a referência
    c.execute("SELECT valor FROM controle WHERE chave='ref_atual'")
    row = c.fetchone()
    if row and row[0] != str(cod_ref):
        print("Nova referência detectada! Limpando banco temporário...")
        c.execute("DELETE FROM marcas")
        c.execute("DELETE FROM modelos")
        c.execute("DELETE FROM anos")
        c.execute("DELETE FROM precos")
        c.execute("UPDATE controle SET valor = ? WHERE chave='ref_atual'", (str(cod_ref),))
        conn.commit()
    elif not row:
         c.execute("INSERT INTO controle (chave, valor) VALUES ('ref_atual', ?)", (str(cod_ref),))
         conn.commit()

    # 2. Coletar Marcas
    for tipo_id, tipo_nome in TIPOS.items():
        c.execute("SELECT count(*) FROM marcas WHERE tipo_veiculo = ?", (tipo_id,))
        if c.fetchone()[0] == 0:
            print(f"Baixando Marcas para {tipo_nome}...")
            payload = {"codigoTabelaReferencia": cod_ref, "codigoTipoVeiculo": tipo_id}
            marcas = make_request("ConsultarMarcas", payload)
            if marcas:
                for m in marcas:
                    c.execute("INSERT OR IGNORE INTO marcas (id, nome, tipo_veiculo, ref_tabela) VALUES (?, ?, ?, ?)",
                              (m['Value'], m['Label'], tipo_id, cod_ref))
                conn.commit()

    # 3. Coletar Modelos
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
                c.execute("INSERT OR IGNORE INTO modelos (id, nome, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, 'PENDENTE')",
                          (mod['Value'], mod['Label'], marca_id, tipo_id, cod_ref))
            conn.commit()

    # 4. Coletar Anos
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
        if anos:
            for ano in anos:
                c.execute("INSERT OR IGNORE INTO anos (codigo, nome, id_modelo, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, ?, 'PENDENTE')",
                          (ano['Value'], ano['Label'], mod_id, marca_id, tipo_id, cod_ref))
        
        c.execute("UPDATE modelos SET status='CONCLUIDO' WHERE id=? AND id_marca=? AND tipo_veiculo=?", (mod_id, marca_id, tipo_id))
        conn.commit()

    # 5. Coletar Preços
    while True:
        check_time()
        c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo FROM anos WHERE status='PENDENTE' LIMIT 1")
        ano_row = c.fetchone()
        if not ano_row:
            break
            
        ano_codigo, ano_nome, mod_id, marca_id, tipo_id = ano_row
        
        try:
            ano_mod, cod_comb = ano_codigo.split('-')
        except:
             c.execute("UPDATE anos SET status='ERRO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?", (ano_codigo, mod_id, tipo_id))
             conn.commit()
             continue

        payload = {
            "codigoTabelaReferencia": cod_ref,
            "codigoTipoVeiculo": tipo_id,
            "codigoMarca": marca_id,
            "codigoModelo": mod_id,
            "anoModelo": int(ano_mod),
            "codigoTipoCombustivel": int(cod_comb),
            "tipoVeiculo": TIPOS[tipo_id],
            "modeloCodigoExterno": "",
            "tipoConsulta": "tradicional"
        }
        
        preco = make_request("ConsultarValorComTodosParametros", payload)
        
        if preco and 'CodigoFipe' in preco:
            # Salvar Preço
            c.execute('''INSERT OR IGNORE INTO precos 
                (codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo, ref_tabela) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                (preco['CodigoFipe'], preco['Marca'], preco['Modelo'], preco['AnoModelo'], preco['Combustivel'], 
                 preco['Valor'], preco['MesReferencia'], tipo_id, cod_ref))
            
            # Atualizar Ano com Código Fipe (importante para o App)
            c.execute("UPDATE anos SET codigo_fipe=?, ano_numerico=?, status='CONCLUIDO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?", 
                      (preco['CodigoFipe'], preco['AnoModelo'], ano_codigo, mod_id, tipo_id))
        else:
            c.execute("UPDATE anos SET status='ERRO' WHERE codigo=? AND id_modelo=? AND tipo_veiculo=?", (ano_codigo, mod_id, tipo_id))
        
        conn.commit()

    # 6. Exportação
    c.execute("SELECT count(*) FROM modelos WHERE status='PENDENTE'")
    pend_mod = c.fetchone()[0]
    c.execute("SELECT count(*) FROM anos WHERE status='PENDENTE'")
    pend_anos = c.fetchone()[0]
    
    if pend_mod == 0 and pend_anos == 0:
        print("Coleta Concluída! Gerando arquivos finais...")
        generate_output_files(c, mes_ref)
        set_github_output("continue_execution", "false") # Avisa que acabou
    else:
        print(f"Ciclo encerrado sem tempo limite (pendentes: Modelos={pend_mod}, Anos={pend_anos}). Reiniciando...")
        set_github_output("continue_execution", "true") # Ainda tem pendências

    conn.close()

def generate_output_files(c, mes_ref):
    # Version
    version_data = {
        "version": f"{datetime.datetime.now().year}-{datetime.datetime.now().month:02d}",
        "fipe_reference": mes_ref,
        "generated_at": datetime.datetime.now().isoformat()
    }
    with open("version.json", "w", encoding="utf-8") as f:
        json.dump(version_data, f, indent=2)
    
    # Exportar tabelas
    # Marcas
    c.execute("SELECT id, nome, tipo_veiculo FROM marcas")
    data = [{"id": r[0], "nome": r[1], "tipo": r[2]} for r in c.fetchall()]
    with gzip.open("fipe_marcas.json.gz", "wt", encoding="utf-8") as f:
        json.dump(data, f)
        
    # Modelos
    c.execute("SELECT id, nome, id_marca, tipo_veiculo FROM modelos")
    data = [{"id": r[0], "nome": r[1], "marca_id": r[2], "tipo": r[3]} for r in c.fetchall()]
    with gzip.open("fipe_modelos.json.gz", "wt", encoding="utf-8") as f:
        json.dump(data, f)
        
    # Anos
    c.execute("SELECT codigo, nome, id_modelo, id_marca, tipo_veiculo, codigo_fipe, ano_numerico FROM anos WHERE status='CONCLUIDO'")
    data = [{"id": r[0], "nome": r[1], "modelo_id": r[2], "marca_id": r[3], "tipo": r[4], "codigo_fipe": r[5], "ano": r[6]} for r in c.fetchall()]
    with gzip.open("fipe_anos.json.gz", "wt", encoding="utf-8") as f:
        json.dump(data, f)
        
    # Preços
    c.execute("SELECT codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo FROM precos")
    data = [{"codigo_fipe": r[0], "marca": r[1], "modelo": r[2], "ano_modelo": r[3], "combustivel": r[4], "valor": r[5], "mes_referencia": r[6], "tipo": r[7]} for r in c.fetchall()]
    with gzip.open("fipe_precos.json.gz", "wt", encoding="utf-8") as f:
        json.dump(data, f)
        
    print("Arquivos gerados com sucesso!")

if __name__ == "__main__":
    run_etl()

