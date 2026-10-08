#!/usr/bin/env python3
# etl/test_coletor_fipe.py
"""
Smoke test para validar a funcionalidade básica do ETL FIPE.
Testa:
  - Criação do banco de dados
  - Sintaxe e importação dos módulos
  - Inserção de dados de teste
  - Geração de arquivos de saída
  - Verificação de integridade dos dados
"""

import os
import sys
import json
import gzip
import sqlite3
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any

# Adiciona o diretório etl ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Testa se o módulo coletor_fipe pode ser importado sem erros."""
    print("✓ Test 1: Importando módulo coletor_fipe...")
    try:
        import coletor_fipe as etl
        print("  ✓ Módulo importado com sucesso")
        return etl
    except Exception as e:
        print(f"  ✗ Erro ao importar: {e}")
        return None


def test_database_creation(etl, temp_dir: str):
    """Testa a criação do banco de dados."""
    print("\n✓ Test 2: Criando banco de dados...")
    try:
        # Override dos paths para usar diretório temporário
        etl.DB_FILE = os.path.join(temp_dir, "temp_data.db")
        etl.VERSION_FILE = os.path.join(temp_dir, "version.json")
        etl.MARCAS_FILE = os.path.join(temp_dir, "fipe_marcas.json.gz")
        etl.MODELOS_FILE = os.path.join(temp_dir, "fipe_modelos.json.gz")
        etl.ANOS_FILE = os.path.join(temp_dir, "fipe_anos.json.gz")
        etl.PRECOS_FILE = os.path.join(temp_dir, "fipe_precos.json.gz")
        
        etl.init_db()
        
        # Verifica se o banco foi criado
        if not os.path.exists(etl.DB_FILE):
            print("  ✗ Banco de dados não foi criado")
            return False
        
        # Verifica as tabelas
        conn = etl.get_connection()
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in c.fetchall()}
        expected_tables = {"controle", "marcas", "modelos", "anos", "precos"}
        
        if not expected_tables.issubset(tables):
            print(f"  ✗ Tabelas faltando: {expected_tables - tables}")
            conn.close()
            return False
        
        conn.close()
        print("  ✓ Banco de dados criado com sucesso")
        print(f"    Tabelas: {', '.join(sorted(tables))}")
        return True
    
    except Exception as e:
        print(f"  ✗ Erro ao criar banco: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_insert_mock_data(etl) -> bool:
    """Testa inserção de dados mock no banco."""
    print("\n✓ Test 3: Inserindo dados de teste...")
    try:
        conn = etl.get_connection()
        c = conn.cursor()
        
        # Insere controle
        cod_ref = 12345
        mes_ref = "Outubro/2026"
        c.execute("INSERT INTO controle (chave, valor) VALUES ('ref_atual', ?)", (str(cod_ref),))
        
        # Insere marcas de teste
        mock_marcas = [
            (1, "Fiat", 1, cod_ref),
            (2, "Volkswagen", 1, cod_ref),
            (3, "Honda", 2, cod_ref),
        ]
        c.executemany(
            "INSERT INTO marcas (id, nome, tipo_veiculo, ref_tabela) VALUES (?, ?, ?, ?)",
            mock_marcas
        )
        
        # Insere modelos de teste
        mock_modelos = [
            (1, "Uno", 1, 1, cod_ref, "PENDENTE"),
            (2, "Gol", 2, 1, cod_ref, "CONCLUIDO"),
        ]
        c.executemany(
            "INSERT INTO modelos (id, nome, id_marca, tipo_veiculo, ref_tabela, status) VALUES (?, ?, ?, ?, ?, ?)",
            mock_modelos
        )
        
        # Insere anos de teste
        mock_anos = [
            ("2024-1", "2024 Gasolina", 1, 1, 1, cod_ref, "ABC1234", 2024, "CONCLUIDO"),
            ("2023-1", "2023 Gasolina", 1, 1, 1, cod_ref, "ABC1235", 2023, "PENDENTE"),
        ]
        c.executemany(
            "INSERT INTO anos (codigo, nome, id_modelo, id_marca, tipo_veiculo, ref_tabela, codigo_fipe, ano_numerico, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            mock_anos
        )
        
        # Insere preços de teste
        mock_precos = [
            ("ABC1234", "Fiat", "Uno", 2024, "Gasolina", "R$ 100.000,00", "Outubro/2026", 1, cod_ref),
            ("ABC1235", "Fiat", "Uno", 2023, "Gasolina", "R$ 95.000,00", "Outubro/2026", 1, cod_ref),
        ]
        c.executemany(
            "INSERT INTO precos (codigo_fipe, marca, modelo, ano_modelo, combustivel, valor, mes_referencia, tipo_veiculo, ref_tabela) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            mock_precos
        )
        
        conn.commit()
        
        # Verifica se os dados foram inseridos
        c.execute("SELECT count(*) FROM marcas")
        num_marcas = c.fetchone()[0]
        c.execute("SELECT count(*) FROM modelos")
        num_modelos = c.fetchone()[0]
        c.execute("SELECT count(*) FROM anos")
        num_anos = c.fetchone()[0]
        c.execute("SELECT count(*) FROM precos")
        num_precos = c.fetchone()[0]
        
        conn.close()
        
        print("  ✓ Dados inseridos com sucesso")
        print(f"    Marcas: {num_marcas}, Modelos: {num_modelos}, Anos: {num_anos}, Preços: {num_precos}")
        return True
    
    except Exception as e:
        print(f"  ✗ Erro ao inserir dados: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_generate_output_files(etl) -> bool:
    """Testa a geração dos arquivos de saída."""
    print("\n✓ Test 4: Gerando arquivos de saída...")
    try:
        conn = etl.get_connection()
        c = conn.cursor()
        
        # Gera os arquivos
        etl.generate_output_files(c, "Outubro/2026", 12345)
        conn.close()
        
        # Verifica se os arquivos foram criados
        files_to_check = [
            etl.VERSION_FILE,
            etl.MARCAS_FILE,
            etl.MODELOS_FILE,
            etl.ANOS_FILE,
            etl.PRECOS_FILE,
        ]
        
        for file in files_to_check:
            if not os.path.exists(file):
                print(f"  ✗ Arquivo não criado: {file}")
                return False
        
        print("  ✓ Todos os arquivos criados com sucesso")
        return True
    
    except Exception as e:
        print(f"  ✗ Erro ao gerar arquivos: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_file_integrity(etl) -> bool:
    """Testa a integridade dos arquivos gerados."""
    print("\n✓ Test 5: Validando integridade dos arquivos...")
    try:
        results = []
        
        # Verifica version.json
        if os.path.exists(etl.VERSION_FILE):
            with open(etl.VERSION_FILE, "r", encoding="utf-8") as f:
                version_data = json.load(f)
                required_keys = {"version", "fipe_reference", "table", "generated_at"}
                if required_keys.issubset(version_data.keys()):
                    print(f"  ✓ version.json válido")
                    results.append(True)
                else:
                    print(f"  ✗ version.json faltando chaves: {required_keys - version_data.keys()}")
                    results.append(False)
        
        # Verifica arquivos .gz
        for file_path, expected_count in [
            (etl.MARCAS_FILE, 3),
            (etl.MODELOS_FILE, 2),
            (etl.ANOS_FILE, 1),  # Apenas os concluídos
            (etl.PRECOS_FILE, 2),
        ]:
            if os.path.exists(file_path):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and len(data) >= 0:
                        print(f"  ✓ {os.path.basename(file_path)} válido ({len(data)} registros)")
                        results.append(True)
                    else:
                        print(f"  ✗ {os.path.basename(file_path)} estrutura inválida")
                        results.append(False)
        
        return all(results)
    
    except Exception as e:
        print(f"  ✗ Erro ao validar integridade: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_functions_exist(etl) -> bool:
    """Verifica se todas as funções necessárias existem."""
    print("\n✓ Test 6: Verificando funções do ETL...")
    try:
        required_functions = [
            "set_github_output",
            "get_connection",
            "init_db",
            "check_time",
            "sleep_checked",
            "throttle",
            "parse_retry_after",
            "schedule_continuation",
            "make_request",
            "get_tabela_referencia",
            "generate_output_files",
            "run_etl",
        ]
        
        missing = []
        for func_name in required_functions:
            if not hasattr(etl, func_name):
                missing.append(func_name)
        
        if missing:
            print(f"  ✗ Funções faltando: {', '.join(missing)}")
            return False
        
        print(f"  ✓ Todas as {len(required_functions)} funções encontradas")
        return True
    
    except Exception as e:
        print(f"  ✗ Erro ao verificar funções: {e}")
        return False


def main():
    """Executa todos os testes."""
    print("=" * 70)
    print("SMOKE TEST - ETL FIPE")
    print("=" * 70)
    
    # Cria diretório temporário
    temp_dir = tempfile.mkdtemp(prefix="etl_test_")
    print(f"\nDiretório temporário: {temp_dir}\n")
    
    try:
        # Test 1: Imports
        etl = test_imports()
        if not etl:
            print("\n✗ Falha no teste de importação. Abortando...")
            return False
        
        # Test 2: Database Creation
        if not test_database_creation(etl, temp_dir):
            print("\n✗ Falha na criação do banco. Abortando...")
            return False
        
        # Test 3: Insert Mock Data
        if not test_insert_mock_data(etl):
            print("\n✗ Falha ao inserir dados. Abortando...")
            return False
        
        # Test 4: Generate Output Files
        if not test_generate_output_files(etl):
            print("\n✗ Falha na geração de arquivos. Abortando...")
            return False
        
        # Test 5: File Integrity
        if not test_file_integrity(etl):
            print("\n✗ Falha na validação de integridade. Abortando...")
            return False
        
        # Test 6: Functions Exist
        if not test_functions_exist(etl):
            print("\n✗ Falha na verificação de funções. Abortando...")
            return False
        
        print("\n" + "=" * 70)
        print("✓ TODOS OS TESTES PASSARAM COM SUCESSO!")
        print("=" * 70)
        return True
    
    except Exception as e:
        print(f"\n✗ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Limpa o diretório temporário
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\nDiretório temporário removido: {temp_dir}")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
