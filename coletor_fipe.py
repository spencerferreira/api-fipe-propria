import requests
import json
import gzip
import os
from datetime import datetime

# URL de uma API pública que já facilita o acesso à FIPE (usamos como fonte)
BASE_URL = "https://parallelum.com.br/fipe/api/v1/carros/marcas"

def buscar_dados():
    print("Iniciando coleta de dados...")
    todos_dados = []
    
    # 1. Pega as marcas
    print("Baixando marcas...")
    response_marcas = requests.get(BASE_URL)
    marcas = response_marcas.json()
    
    # PARA TESTE: Vamos limitar a 2 marcas para ser rápido. 
    # No real, você removeria o [:2]
    for marca in marcas: 
        print(f"Processando marca: {marca['nome']}")
        
        dados_marca = {
            "codigo": marca['codigo'],
            "nome": marca['nome'],
            "tipoVeiculoId": 1, # 1 = Carro
            "modelos": []
        }
        
        # 2. Pega modelos da marca
        url_modelos = f"{BASE_URL}/{marca['codigo']}/modelos"
        resp_modelos = requests.get(url_modelos)
        lista_modelos = resp_modelos.json()['modelos']
        
        for modelo in lista_modelos: # Limitando a 3 modelos para teste
            dados_modelo = {
                "codigo": modelo['codigo'],
                "nome": modelo['nome'],
                "anos": []
            }
            
            # 3. Pega anos do modelo
            url_anos = f"{url_modelos}/{modelo['codigo']}/anos"
            resp_anos = requests.get(url_anos)
            lista_anos = resp_anos.json()
            
            for ano in lista_anos:
                # 4. Pega o valor final (Preço)
                url_valor = f"{url_anos}/{ano['codigo']}"
                resp_valor = requests.get(url_valor)
                valor_fipe = resp_valor.json()
                
                # Monta o objeto final igual ao que seu App Android espera (MarcaImport)
                dados_ano_preco = {
                    "codigo": ano['codigo'],
                    "nome": ano['nome'],
                    "preco": {
                        "codigoFipe": valor_fipe['CodigoFipe'],
                        "marca": valor_fipe['Marca'],
                        "modelo": valor_fipe['Modelo'],
                        "anoModelo": valor_fipe['AnoModelo'],
                        "combustivel": valor_fipe['Combustivel'],
                        "valor": valor_fipe['Valor'],
                        "mesReferencia": valor_fipe['MesReferencia'],
                        "tipoVeiculoId": 1
                    }
                }
                dados_modelo['anos'].append(dados_ano_preco)
            
            dados_marca['modelos'].append(dados_modelo)
        
        todos_dados.append(dados_marca)

    return todos_dados

def salvar_arquivos(dados):
    # Salva o JSON puro (para leitura humana se precisar)
    with open("fipe_db.json", "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
        
    # Salva compactado em GZIP (para o App baixar rápido)
    with gzip.open("fipe_db.json.gz", "wt", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)
        
    # Cria o arquivo de versão
    versao_info = {
        "version": datetime.now().strftime("%Y%m%d"), # Ex: 20251130
        "date": datetime.now().strftime("%Y-%m-%d")
    }
    with open("version.json", "w", encoding="utf-8") as f:
        json.dump(versao_info, f)

    print("Arquivos gerados com sucesso!")

if __name__ == "__main__":
    dados = buscar_dados()
    salvar_arquivos(dados)
