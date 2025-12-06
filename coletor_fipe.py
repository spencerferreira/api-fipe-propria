import requests
import json
import gzip
import time
import random
from datetime import datetime

# ================= CONFIGURAÇÕES DA API OFICIAL FIPE =================
BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"

# Headers obrigatórios para a API oficial aceitar a conexão
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://veiculos.fipe.org.br",
    "Referer": "https://veiculos.fipe.org.br/",
    "X-Requested-With": "XMLHttpRequest"
}

# Tipos de veículo conforme padrão da FIPE
TIPOS_VEICULO = {
    "carros": 1,
    "motos": 2,
    "caminhoes": 3
}

# Sessão global para manter cookies e conexão
session = requests.Session()
session.headers.update(HEADERS)

# ================= FUNÇÕES DE REDE (Baseadas no seu fipe_precos.py) =================

def sleep_random(min_s=1.0, max_s=2.0):
    """Pausa aleatória para parecer humano e evitar bloqueio"""
    time.sleep(random.uniform(min_s, max_s))

def post_request(endpoint, data=None, tentativas=5):
    """Faz requisições POST robustas à API Oficial"""
    url = f"{BASE_URL}{endpoint}"
    
    for i in range(tentativas):
        try:
            # Pausa antes de cada requisição
            sleep_random(0.5, 1.5)
            
            response = session.post(url, data=data, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = (i + 1) * 5
                print(f"   🚦 API pediu pausa (429). Esperando {wait}s...")
                time.sleep(wait)
            else:
                print(f"   ⚠️ Erro {response.status_code} em {endpoint}. Tentativa {i+1}/{tentativas}")
                time.sleep(2)
                
        except Exception as e:
            print(f"   ❌ Erro de conexão: {e}. Tentativa {i+1}/{tentativas}")
            time.sleep(5)
            
    print(f"   ☠️ DESISTINDO de {endpoint} após falhas consecutivas.")
    return None

# ================= LÓGICA DE COLETA =================

def obter_codigo_tabela_referencia():
    """Pega o código da tabela FIPE mais recente (ex: 306 para fev/2024)"""
    print("📅 Buscando tabela de referência mais recente...")
    res = post_request("/ConsultarTabelaDeReferencia")
    if res and isinstance(res, list) and len(res) > 0:
        codigo = res[0]['Codigo']
        mes = res[0]['Mes']
        print(f"✅ Tabela encontrada: {codigo} - {mes}")
        return codigo
    raise Exception("Não foi possível obter a tabela de referência.")

def buscar_dados_completos():
    cod_tabela = obter_codigo_tabela_referencia()
    todos_dados = []

    for tipo_nome, tipo_id in TIPOS_VEICULO.items():
        print(f"\n=========================================\n🚛 Iniciando coleta de: {tipo_nome.upper()}\n=========================================")
        
        # 1. Consultar Marcas
        payload_marcas = {
            "codigoTabelaReferencia": cod_tabela,
            "codigoTipoVeiculo": tipo_id
        }
        marcas = post_request("/ConsultarMarcas", payload_marcas)
        
        if not marcas:
            print(f"❌ Nenhuma marca encontrada para {tipo_nome}")
            continue

        for i, marca in enumerate(marcas):
            marca_id = marca['Value']
            marca_nome = marca['Label']
            print(f"🏭 [{i+1}/{len(marcas)}] {marca_nome} ({tipo_nome})...")

            dados_marca = {
                "codigo": str(marca_id),
                "nome": marca_nome,
                "tipoVeiculoId": tipo_id,
                "modelos": []
            }

            # 2. Consultar Modelos da Marca
            payload_modelos = {
                "codigoTabelaReferencia": cod_tabela,
                "codigoTipoVeiculo": tipo_id,
                "codigoMarca": marca_id
            }
            resp_modelos = post_request("/ConsultarModelos", payload_modelos)
            
            if not resp_modelos or 'Modelos' not in resp_modelos:
                todos_dados.append(dados_marca) # Salva a marca mesmo sem modelos
                continue

            modelos_lista = resp_modelos['Modelos']
            
            for modelo in modelos_lista:
                modelo_id = modelo['Value']
                modelo_nome = modelo['Label']
                
                dados_modelo = {
                    "codigo": modelo_id, # Android espera Int aqui (API oficial manda int no Value)
                    "nome": modelo_nome,
                    "anos": []
                }

                # 3. Consultar Anos do Modelo
                payload_anos = {
                    "codigoTabelaReferencia": cod_tabela,
                    "codigoTipoVeiculo": tipo_id,
                    "codigoMarca": marca_id,
                    "codigoModelo": modelo_id
                }
                anos_lista = post_request("/ConsultarAnoModelo", payload_anos)

                if anos_lista:
                    for ano in anos_lista:
                        ano_codigo = ano['Value'] # Ex: "2014-1"
                        ano_nome = ano['Label']   # Ex: "2014 Gasolina"

                        # 4. Consultar Preço Final
                        payload_valor = {
                            "codigoTabelaReferencia": cod_tabela,
                            "codigoTipoVeiculo": tipo_id,
                            "codigoMarca": marca_id,
                            "codigoModelo": modelo_id,
                            "anoModelo": ano_codigo,
                            "codigoTipoCombustivel": ano_codigo.split('-')[1] if '-' in ano_codigo else 1,
                            "tipoConsulta": "tradicional"
                        }
                        
                        valor_fipe = post_request("/ConsultarValorComTodosParametros", payload_valor)

                        if valor_fipe:
                            # Mapeia para o formato do App Android
                            dados_ano = {
                                "codigo": ano_codigo,
                                "nome": ano_nome,
                                "preco": {
                                    "codigoFipe": valor_fipe.get('CodigoFipe', ''),
                                    "marca": valor_fipe.get('Marca', ''),
                                    "modelo": valor_fipe.get('Modelo', ''),
                                    "anoModelo": valor_fipe.get('AnoModelo', 0),
                                    "combustivel": valor_fipe.get('Combustivel', ''),
                                    "valor": valor_fipe.get('Valor', ''),
                                    "mesReferencia": valor_fipe.get('MesReferencia', ''),
                                    "tipoVeiculoId": tipo_id
                                }
                            }
                            dados_modelo['anos'].append(dados_ano)
                
                dados_marca['modelos'].append(dados_modelo)
            
            todos_dados.append(dados_marca)
            
    return todos_dados

def salvar_arquivos(dados):
    if not dados:
        print("❌ Nenhum dado coletado.")
        return

    print(f"\n💾 Salvando arquivo final com {len(dados)} marcas...")
    
    # Salva JSON e GZIP
    json_str = json.dumps(dados, ensure_ascii=False)
    with open("fipe_db.json", "w", encoding="utf-8") as f:
        f.write(json_str)
    with gzip.open("fipe_db.json.gz", "wt", encoding="utf-8") as f:
        f.write(json_str)

    # Versão
    versao_info = {
        "version": datetime.now().strftime("%Y%m%d.%H%M"),
        "date": datetime.now().isoformat()
    }
    with open("version.json", "w", encoding="utf-8") as f:
        json.dump(versao_info, f)
        
    print("✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    dados = buscar_dados_completos()
    salvar_arquivos(dados)
