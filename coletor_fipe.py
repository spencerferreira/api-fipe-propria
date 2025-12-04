import requests
import json
import gzip
import time
import os
from datetime import datetime

# URL base da API
BASE_URL = "https://parallelum.com.br/fipe/api/v1/carros/marcas"

# Função auxiliar para fazer requisições com segurança (tentativas automáticas)
def fazer_requisicao(url, tentativas=3):
    for i in range(tentativas):
        try:
            response = requests.get(url, timeout=10) # Timeout evita que trave para sempre
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429: # Erro "Muitas requisições"
                print(f"⚠️ Calma! O servidor pediu pausa. Esperando 5s... (Tentativa {i+1}/{tentativas})")
                time.sleep(5)
            else:
                print(f"⚠️ Erro {response.status_code} ao acessar {url}")
        
        except Exception as e:
            print(f"⚠️ Erro de conexão: {str(e)}. Tentando novamente...")
        
        time.sleep(1) # Pausa de 1 segundo antes de tentar de novo
        
    return None # Desiste se falhar 3 vezes

def buscar_dados():
    print("🚀 Iniciando coleta COMPLETA e BLINDADA de dados...")
    todos_dados = []
    
    # 1. Pega as marcas
    print("📡 Baixando lista de marcas...")
    marcas = fazer_requisicao(BASE_URL)
    
    if not marcas:
        print("❌ Falha crítica: Não foi possível baixar as marcas. Abortando.")
        return []

    # Percorre TODAS as marcas
    for i, marca in enumerate(marcas):
        print(f"🏭 [{i+1}/{len(marcas)}] Processando marca: {marca['nome']}...")
        
        dados_marca = {
            "codigo": marca['codigo'],
            "nome": marca['nome'],
            "tipoVeiculoId": 1,
            "modelos": []
        }
        
        # 2. Pega modelos da marca
        url_modelos = f"{BASE_URL}/{marca['codigo']}/modelos"
        resp_modelos = fazer_requisicao(url_modelos)
        
        if not resp_modelos:
            print(f"   -> Pulei a marca {marca['nome']} (falha ao obter modelos)")
            continue

        lista_modelos = resp_modelos.get('modelos', [])
        
        for j, modelo in enumerate(lista_modelos):
            # Log para acompanhar progresso (ajuda a ver se não travou)
            if j % 10 == 0: print(f"   🚗 Processando modelo {j+1}/{len(lista_modelos)}: {modelo['nome']}")

            dados_modelo = {
                "codigo": modelo['codigo'],
                "nome": modelo['nome'],
                "anos": []
            }
            
            # 3. Pega anos do modelo
            url_anos = f"{url_modelos}/{modelo['codigo']}/anos"
            lista_anos = fazer_requisicao(url_anos)
            
            if lista_anos:
                for ano in lista_anos:
                    # 4. Pega o valor final (Preço)
                    url_valor = f"{url_anos}/{ano['codigo']}"
                    valor_fipe = fazer_requisicao(url_valor)
                    
                    if valor_fipe:
                        dados_ano_preco = {
                            "codigo": ano['codigo'],
                            "nome": ano['nome'],
                            "preco": {
                                "codigoFipe": valor_fipe.get('CodigoFipe', ''),
                                "marca": valor_fipe.get('Marca', ''),
                                "modelo": valor_fipe.get('Modelo', ''),
                                "anoModelo": valor_fipe.get('AnoModelo', 0),
                                "combustivel": valor_fipe.get('Combustivel', ''),
                                "valor": valor_fipe.get('Valor', ''),
                                "mesReferencia": valor_fipe.get('MesReferencia', ''),
                                "tipoVeiculoId": 1
                            }
                        }
                        dados_modelo['anos'].append(dados_ano_preco)
                    
                    # Pausa tática para não derrubar a API (muito importante!)
                    time.sleep(0.1) 

            dados_marca['modelos'].append(dados_modelo)
        
        todos_dados.append(dados_marca)

    return todos_dados

def salvar_arquivos(dados):
    if not dados:
        print("❌ Nenhum dado foi coletado. Nada a salvar.")
        return

    print(f"💾 Salvando {len(dados)} marcas no arquivo...")
    
    with open("fipe_db.json", "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
        
    with gzip.open("fipe_db.json.gz", "wt", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)
        
    versao_info = {
        "version": datetime.now().strftime("%Y%m%d"),
        "date": datetime.now().strftime("%Y-%m-%d")
    }
    with open("version.json", "w", encoding="utf-8") as f:
        json.dump(versao_info, f)

    print("✅ SUCESSO! Arquivos fipe_db.json.gz e version.json gerados.")

if __name__ == "__main__":
    dados = buscar_dados()
    salvar_arquivos(dados)
