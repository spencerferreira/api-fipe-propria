
import requests
import json
import gzip
import time
from datetime import datetime

# Lista de tipos de veículo que vamos buscar e seus IDs no app
TIPOS_VEICULO = {
    "carros": 1,
    "motos": 2,
    "caminhoes": 3
}

# --- FUNÇÕES AUXILIARES ---

def fazer_requisicao(url, tentativas=5, pausa_base=2):
    """
    Função 'trator' para fazer requisições de forma resiliente.
    Tenta várias vezes com pausas crescentes se a API bloquear.
    """
    for i in range(tentativas):
        try:
            response = requests.get(url, timeout=30)  # Timeout de 30s
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Fomos bloqueados temporariamente
                pausa_atual = pausa_base * (i + 1)
                print(f"   -> 🚦 API pediu para aguardar. Pausando por {pausa_atual}s...")
                time.sleep(pausa_atual)
            else:
                # Outros erros de servidor (500, 503, etc.)
                print(f"   -> ⚠️  Aviso: Erro {response.status_code} em {url}. Tentando de novo...")
                time.sleep(pausa_base)

        except requests.exceptions.RequestException as e:
            print(f"   -> Erro de Conectividade: {e}. Tentando novamente...")
            time.sleep(pausa_base * (i + 1))
            
    print(f"   -> ❌ FALHA FINAL após {tentativas} tentativas para a URL: {url}")
    return None

def salvar_arquivos(dados):
    """
    Salva os dados coletados nos arquivos finais (JSON e GZIP).
    """
    if not dados:
        print("\n❌ Nenhum dado foi coletado. Nenhum arquivo será salvo.")
        return

    print(f"\n💾 Preparando para salvar dados de {len(dados)} marcas...")
    
    try:
        # Usamos ensure_ascii=False para salvar acentos corretamente
        json_str = json.dumps(dados, ensure_ascii=False)
        
        # Salva o arquivo GZIP para o app
        with gzip.open("fipe_db.json.gz", "wt", encoding="utf-8") as f:
            f.write(json_str)

        # Cria o arquivo de versão
        versao_info = {
            "version": datetime.now().strftime("%Y%m%d.%H%M"),
            "date": datetime.now().isoformat()
        }
        with open("version.json", "w", encoding="utf-8") as f:
            json.dump(versao_info, f)

        print("✅ SUCESSO! Arquivos fipe_db.json.gz e version.json gerados com a base completa.")
        
    except Exception as e:
        print(f"❌ Erro crítico ao salvar os arquivos: {e}")

# --- FUNÇÃO PRINCIPAL ---

def buscar_dados_completos():
    """
    Orquestrador principal que busca todos os tipos de veículos e junta os resultados.
    """
    print("🚜 Iniciando coleta MESTRE (Carros, Motos, Caminhões)...")
    todos_os_dados = []

    for tipo_veiculo, tipo_id in TIPOS_VEICULO.items():
        print(f"\n=========================================\n FASE: Buscando '{tipo_veiculo.upper()}'\n=========================================")
        
        url_marcas = f"https://parallelum.com.br/fipe/api/v1/{tipo_veiculo}/marcas"
        marcas = fazer_requisicao(url_marcas)
        
        if not marcas:
            print(f"⚠️ Não foi possível obter marcas para o tipo '{tipo_veiculo}', pulando.")
            continue

        for i, marca in enumerate(marcas):
            print(f"  -> Processando marca {i+1}/{len(marcas)}: {marca['nome']} ({tipo_veiculo})")
            
            # Estrutura para os dados da marca atual
            dados_marca_atual = {
                "codigo": marca['codigo'],
                "nome": marca['nome'],
                "tipoVeiculoId": tipo_id,
                "modelos": []
            }
            
            url_modelos = f"{url_marcas}/{marca['codigo']}/modelos"
            resp_modelos = fazer_requisicao(url_modelos)
            
            if resp_modelos and 'modelos' in resp_modelos:
                lista_modelos = resp_modelos['modelos']
                for modelo in lista_modelos:
                    dados_modelo_atual = {
                        "codigo": modelo['codigo'],
                        "nome": modelo['nome'],
                        "anos": []
                    }

                    url_anos = f"{url_modelos}/{modelo['codigo']}/anos"
                    lista_anos = fazer_requisicao(url_anos)

                    if lista_anos:
                        for ano in lista_anos:
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
                                        "tipoVeiculoId": tipo_id
                                    }
                                }
                                dados_modelo_atual['anos'].append(dados_ano_preco)
                            # Pequena pausa para não sobrecarregar a API
                            time.sleep(0.02) 

                    # Adiciona os modelos à marca atual
                    dados_marca_atual['modelos'].append(dados_modelo_atual)
            
            # Adiciona a marca (com todos os seus modelos) à lista final
            todos_os_dados.append(dados_marca_atual)
    
    return todos_os_dados

# --- Ponto de Entrada ---
if __name__ == "__main__":
    dados_finais = buscar_dados_completos()
    salvar_arquivos(dados_finais)
