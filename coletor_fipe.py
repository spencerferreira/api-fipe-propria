import requests
import json
import gzip
import time
import random
from datetime import datetime

# ================= CONFIGURAÇÕES DA API OFICIAL FIPE =================
BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://veiculos.fipe.org.br",
    "Referer": "https://veiculos.fipe.org.br/",
    "X-Requested-With": "XMLHttpRequest"
}
TIPOS_VEICULO = {"carros": 1, "motos": 2, "caminhoes": 3}
session = requests.Session()
session.headers.update(HEADERS)

# ================= FUNÇÕES DE REDE =================
def sleep_random(min_s=0.2, max_s=0.5):
    """Pausa pequena é suficiente, pois não faremos milhares de requisições de preço."""
    time.sleep(random.uniform(min_s, max_s))

def post_request(endpoint, data=None, tentativas=5):
    url = f"{BASE_URL}{endpoint}"
    for i in range(tentativas):
        try:
            sleep_random()
            response = session.post(url, data=data, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = (i + 1) * 2
                print(f"   -> 🚦 API pediu pausa (429). Esperando {wait}s...")
                time.sleep(wait)
            else:
                print(f"   -> ⚠️  Aviso: Erro {response.status_code} em {endpoint}. Tentativa {i+1}/{tentativas}")
                time.sleep(1)
        except Exception as e:
            print(f"   -> ❌ Erro de conexão: {e}. Tentativa {i+1}/{tentativas}")
            time.sleep(2)
    return None

# ================= LÓGICA DE COLETA ESTRUTURAL =================
def obter_codigo_tabela_referencia():
    print("📅 Buscando tabela de referência mais recente...")
    res = post_request("/ConsultarTabelaDeReferencia")
    if res and isinstance(res, list) and len(res) > 0:
        codigo, mes = res[0]['Codigo'], res[0]['Mes']
        print(f"✅ Tabela encontrada: {codigo} - {mes}")
        return codigo
    raise Exception("Não foi possível obter a tabela de referência.")

def buscar_dados_estruturais():
    cod_tabela = obter_codigo_tabela_referencia()
    todos_dados = []

    for tipo_nome, tipo_id in TIPOS_VEICULO.items():
        print(f"\n====================\n🚛 Coletando ESTRUTURA: {tipo_nome.upper()}\n====================")
        
        # 1. Marcas
        marcas = post_request("/ConsultarMarcas", {"codigoTabelaReferencia": cod_tabela, "codigoTipoVeiculo": tipo_id})
        if not marcas: continue

        for i, marca in enumerate(marcas):
            marca_id, marca_nome = marca['Value'], marca['Label']
            print(f"  -> Marca {i+1}/{len(marcas)}: {marca_nome} ({tipo_nome})")
            
            dados_marca = {
                "codigo": str(marca_id),
                "nome": marca_nome,
                "tipoVeiculoId": tipo_id,
                "modelos": []
            }

            # 2. Modelos
            resp_modelos = post_request("/ConsultarModelos", {
                "codigoTabelaReferencia": cod_tabela,
                "codigoTipoVeiculo": tipo_id,
                "codigoMarca": marca_id
            })
            
            if not resp_modelos or 'Modelos' not in resp_modelos:
                todos_dados.append(dados_marca) # Salva a marca mesmo que não tenha modelos
                continue
            
            for modelo in resp_modelos['Modelos']:
                modelo_id, modelo_nome = modelo['Value'], modelo['Label']
                
                dados_modelo = {
                    "codigo": modelo_id,
                    "nome": modelo_nome,
                    "anos": []
                }

                # 3. Anos (SOMENTE A LISTA DE ANOS, SEM PREÇO)
                anos_lista = post_request("/ConsultarAnoModelo", {
                    "codigoTabelaReferencia": cod_tabela,
                    "codigoTipoVeiculo": tipo_id,
                    "codigoMarca": marca_id,
                    "codigoModelo": modelo_id
                })

                if anos_lista:
                    for ano in anos_lista:
                        # Salvamos apenas o código e nome do ano.
                        # O campo "preco" é nulo, pois será buscado pelo App sob demanda.
                        dados_ano = {
                            "codigo": ano['Value'], # Ex: "2014-1"
                            "nome": ano['Label'],   # Ex: "2014 Gasolina"
                            "preco": None # NÃO BUSCAMOS O PREÇO AQUI!
                        }
                        dados_modelo['anos'].append(dados_ano)
                
                dados_marca['modelos'].append(dados_modelo)
            
            todos_dados.append(dados_marca)
            
    return todos_dados

def salvar_arquivos(dados):
    if not dados:
        print("\n❌ Nenhum dado foi coletado.")
        return
    
    print(f"\n💾 Salvando arquivo ESTRUTURAL com {len(dados)} marcas...")
    json_str = json.dumps(dados, ensure_ascii=False)
    
    with open("fipe_db.json", "w", encoding="utf-8") as f: f.write(json_str)
    with gzip.open("fipe_db.json.gz", "wt", encoding="utf-8") as f: f.write(json_str)
    
    versao_info = {"version": datetime.now().strftime("%Y%m%d.%H%M"), "date": datetime.now().isoformat()}
    with open("version.json", "w", encoding="utf-8") as f: json.dump(versao_info, f)
    
    print("✅ Processo concluído! Arquivo leve e rápido gerado.")

if __name__ == "__main__":
    dados = buscar_dados_estruturais()
    salvar_arquivos(dados)
