import requests
import json
import gzip
import time
import os
from datetime import datetime

# Lista de tipos de veículo que vamos buscar e seus IDs
TIPOS_VEICULO = {
    "carros": 1,
    "motos": 2,
    "caminhoes": 3
}

# Função auxiliar para fazer requisições com segurança
def fazer_requisicao(url, tentativas=3):
    for i in range(tentativas):
        try:
            response = requests.get(url, timeout=20)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"   -> ⚠️  API pediu pausa. Esperando 10s... (URL: {url})")
                time.sleep(10)
            else:
                print(f"   -> ⚠️  Erro {response.status_code} ao acessar {url}")
        except Exception as e:
            print(f"   -> ⚠️  Erro de conexão: {str(e)}. Tentando novamente...")
        time.sleep(2)
    return None

def buscar_dados():
    print("🚀 Iniciando coleta MESTRE (Carros, Motos, Caminhões)...")
    todos_dados_finais = []

    # Loop principal para cada tipo de veículo
    for tipo, tipo_id in TIPOS_VEICULO.items():
        print(f"\n======================\n🚛 Buscando TIPO: {tipo.upper()}\n======================")
        base_url = f"https://parallelum.com.br/fipe/api/v1/{tipo}/marcas"

        # 1. Pega as marcas do tipo atual
        marcas = fazer_requisicao(base_url)
