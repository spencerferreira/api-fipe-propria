import requests
import json
import gzip
import time
import random
from typing import Dict, List, Any, Optional

# ========================= Config & Sessão (Baseado em fipe_precos.py) =========================

BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://veiculos.fipe.org.br",
    "Referer": "https://veiculos.fipe.org.br/",
}

session = requests.Session()
session.headers.update(HEADERS)

# Auto-throttle e tentativas
auto_delay = 1.5
MAX_ATTEMPTS = 5

# ========================= HTTP Resiliente (Baseado em fipe_precos.py) =========================

def sleep_with_jitter(base: float):
    """Aguarda por um tempo base com uma variação para evitar padrões."""
    jitter = base * 0.2
    time.sleep(max(0.0, base + random.uniform(-jitter, jitter)))

def post_json_resiliente(path: str, data: dict, max_attempts: int = MAX_ATTEMPTS) -> Optional[Any]:
    """Faz um POST para a API FIPE com lógica de retentativa e backoff."""
    global auto_delay
    url = BASE_URL + path
    attempt = 0
    backoff_sequence = [2.0, 4.0, 8.0, 12.0, 20.0]

    while attempt < max_attempts:
        try:
            sleep_with_jitter(auto_delay)
            r = session.post(url, data=data, timeout=30.0)

            if r.status_code in (429, 503):
                wait_s = backoff_sequence[min(attempt, len(backoff_sequence) - 1)]
                print(f"  [AVISO] {r.status_code} em {path}. Aguardando {wait_s:.1f}s...")
                sleep_with_jitter(wait_s)
                auto_delay = min(auto_delay * 1.5, 5.0) # Aumenta o delay
                attempt += 1
                continue

            r.raise_for_status()
            auto_delay = max(1.5, auto_delay * 0.9) # Reduz o delay se bem sucedido
            return r.json()

        except requests.exceptions.RequestException as e:
            wait_s = backoff_sequence[min(attempt, len(backoff_sequence) - 1)]
            print(f"  [ERRO] Requisição para {url} falhou: {e}. Retentando em {wait_s:.1f}s...")
            attempt += 1
        
        except json.JSONDecodeError:
            print(f"  [ERRO] Resposta não-JSON de {url}. Conteúdo: {r.text[:100]}")
            return None

    print(f"  [FALHA] Falha definitiva em {url} após {max_attempts} tentativas.")
    return None

# ========================= FIPE Core (Baseado em fipe_precos.py) =========================

def consultar_tabela_referencia() -> Optional[int]:
    """Busca a tabela de referência mais recente."""
    res = post_json_resiliente("/ConsultarTabelaDeReferencia", data={})
    if res and isinstance(res, list) and len(res) > 0:
        return res[0]['Codigo']
    return None

def consultar_marcas(cod_tabela: int, tipo_veiculo: int) -> List[Dict[str, str]]:
    data = {"codigoTabelaReferencia": cod_tabela, "codigoTipoVeiculo": tipo_veiculo}
    res = post_json_resiliente("/ConsultarMarcas", data)
    return res or []

def consultar_modelos(cod_tabela: int, tipo_veiculo: int, cod_marca: str) -> List[Dict[str, Any]]:
    data = {
        "codigoTabelaReferencia": cod_tabela,
        "codigoTipoVeiculo": tipo_veiculo,
        "codigoMarca": cod_marca,
    }
    res = post_json_resiliente("/ConsultarModelos", data)
    return (res.get('Modelos', []) if isinstance(res, dict) else []) or []


def consultar_anos(cod_tabela: int, tipo_veiculo: int, cod_marca: str, cod_modelo: str) -> List[Dict[str, str]]:
    data = {
        "codigoTabelaReferencia": cod_tabela,
        "codigoTipoVeiculo": tipo_veiculo,
        "codigoMarca": cod_marca,
        "codigoModelo": cod_modelo,
    }
    res = post_json_resiliente("/ConsultarAnoModelo", data)
    return res or []

def consultar_valor(cod_tabela: int, tipo_veiculo: int, cod_marca: str, cod_modelo: str, ano_comb: str) -> Optional[Dict[str, str]]:
    ano, comb = ano_comb.split('-')
    data = {
        "codigoTabelaReferencia": cod_tabela,
        "codigoTipoVeiculo": tipo_veiculo,
        "codigoMarca": cod_marca,
        "codigoModelo": cod_modelo,
        "anoModelo": ano,
        "codigoTipoCombustivel": comb,
        "tipoConsulta": "tradicional",
    }
    return post_json_resiliente("/ConsultarValorComTodosParametros", data)

# ========================= Lógica de Coleta e Salvamento =========================

def coletar_marcas_por_fipe() -> Dict[str, str]:
    """
    Coleta todos os códigos FIPE e suas respectivas marcas da API oficial da FIPE.
    """
    db_marca: Dict[str, str] = {}
    tipos = [("Carros", 1), ("Motos", 2), ("Caminhões", 3)]
    
    print("Buscando tabela de referência mais recente...")
    cod_tabela = consultar_tabela_referencia()
    if not cod_tabela:
        print("Não foi possível obter a tabela de referência. Abortando.")
        return {}
    print(f"Tabela de referência: {cod_tabela}\n")

    for tipo_nome, tipo_id in tipos:
        print(f"--- Coletando Marcas para: {tipo_nome} ---")
        marcas = consultar_marcas(cod_tabela, tipo_id)
        
        for i, marca in enumerate(marcas):
            marca_nome = marca['Label']
            marca_id = marca['Value']
            print(f"  ({i+1}/{len(marcas)}) Processando marca: {marca_nome}")
            
            modelos = consultar_modelos(cod_tabela, tipo_id, marca_id)
            
            for j, modelo in enumerate(modelos):
                modelo_id = modelo['Value']
                # Log reduzido para não poluir tanto o terminal
                if (j + 1) % 10 == 0 or j == len(modelos) -1:
                    print(f"    - Processando modelo {j+1}/{len(modelos)}...")

                anos = consultar_anos(cod_tabela, tipo_id, marca_id, modelo_id)
                
                for ano in anos:
                    ano_id = ano['Value']
                    
                    veiculo = consultar_valor(cod_tabela, tipo_id, marca_id, modelo_id, ano_id)
                    
                    if veiculo and 'CodigoFipe' in veiculo and 'Marca' in veiculo:
                        codigo_fipe = veiculo['CodigoFipe']
                        marca_veiculo = veiculo['Marca']
                        
                        if codigo_fipe not in db_marca:
                            db_marca[codigo_fipe] = marca_veiculo
                            # Opcional: descomente para ver cada novo registro
                            # print(f"      -> Novo registro: {codigo_fipe}: {marca_veiculo}")
                        
    return db_marca

def salvar_banco_gzip(dados: Dict, arquivo: str):
    """Salva os dados em um arquivo JSON comprimido com Gzip."""
    
    # Ordena o dicionário pelo código FIPE para consistência
    dados_ordenados = dict(sorted(dados.items()))

    with gzip.open(arquivo, 'wt', encoding='utf-8') as f:
        json.dump(dados_ordenados, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Banco de dados salvo com sucesso: {arquivo}")
    print(f"  Total de registros únicos: {len(dados)}")


if __name__ == "__main__":
    print("Iniciando coletor de Marcas por Código FIPE...")
    print("Este processo pode levar MUITOS minutos devido à quantidade de requisições.\n")
    
    db = coletar_marcas_por_fipe()
    
    if db:
        salvar_banco_gzip(db, "fipe_marca_db.json.gz")
    else:
        print("Nenhum dado foi coletado. O arquivo não foi gerado.")
