"""
Script DEBUG para Cloud Run - Instagram Stories Capture
Versão com logs detalhados e delay aumentado
"""

import os
import pandas as pd
import duckdb
from google.cloud import storage
import tempfile
from datetime import datetime
from instagram_network_capture_debug import capturar_multiplas_paginas

def download_from_gcs(bucket_name, source_blob_name, destination_file):
    """Download de arquivo do Google Cloud Storage"""
    try:
        print(f"Bucket: {bucket_name}")
        print(f"Arquivo GCS: {source_blob_name}")
        print(f"Destino local: {destination_file}")
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        # Verificar se o blob existe
        if not blob.exists():
            print(f"ERRO: Arquivo não existe no GCS: gs://{bucket_name}/{source_blob_name}")
            return False
        
        # Download
        blob.download_to_filename(destination_file)
        
        # Verificar tamanho
        file_size = os.path.getsize(destination_file)
        print(f"✓ Download concluído: {file_size} bytes")
        print(f"  gs://{bucket_name}/{source_blob_name}")
        
        return True
    except Exception as e:
        print(f"✗ Erro no download GCS: {e}")
        import traceback
        traceback.print_exc()
        return False

def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    """Upload de arquivo para o Google Cloud Storage"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)
        print(f"Uploaded to GCS: gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"Erro no upload GCS: {e}")
        return False

def processar_json_com_duckdb(json_files, output_csv):
    """Processa múltiplos arquivos JSON usando DuckDB e gera CSV consolidado"""
    try:
        con = duckdb.connect(':memory:')
        
        # Preparar lista de arquivos para query
        files_str = ', '.join([f"'{f}'" for f in json_files])
        
        query = f"""
        WITH raw_data AS (
            SELECT 
                REPLACE(REPLACE(filename, '.json', ''), '_stories', '') as usuario,
                unnest(json->'require') as item
            FROM read_json([{files_str}], 
                          filename=true,
                          ignore_errors=true)
        ),
        extracted_data AS (
            SELECT 
                usuario,
                unnest(item[4][1]) as story_item
            FROM raw_data
            WHERE len(item) > 4
              AND item[4] IS NOT NULL
              AND len(item[4]) > 1
        ),
        links_data AS (
            SELECT 
                usuario,
                story_item->'node'->>'id' as story_id,
                unnest(story_item->'node'->'story_cta') as cta
            FROM extracted_data
            WHERE story_item->'node'->'story_cta' IS NOT NULL
        )
        SELECT DISTINCT
            usuario,
            story_id,
            cta[2]->>'link'->>'url' as link,
            CASE
                WHEN cta[2]->>'link'->>'url' LIKE '%amazon.%' THEN 'Amazon'
                WHEN cta[2]->>'link'->>'url' LIKE '%mercadolivre.%' THEN 'Mercado Livre'
                WHEN cta[2]->>'link'->>'url' LIKE '%mercadolibre.%' THEN 'Mercado Livre'
                WHEN cta[2]->>'link'->>'url' LIKE '%shopee.%' THEN 'Shopee'
                WHEN cta[2]->>'link'->>'url' LIKE '%aliexpress.%' THEN 'AliExpress'
                WHEN cta[2]->>'link'->>'url' LIKE '%magazineluiza.%' THEN 'Magazine Luiza'
                WHEN cta[2]->>'link'->>'url' LIKE '%casasbahia.%' THEN 'Casas Bahia'
                WHEN cta[2]->>'link'->>'url' LIKE '%americanas.%' THEN 'Americanas'
                ELSE 'Outro'
            END as marketplace
        FROM links_data
        WHERE cta[2]->>'link'->>'url' IS NOT NULL
        ORDER BY usuario, story_id
        """
        
        df = con.execute(query).fetchdf()
        con.close()
        
        # Salvar CSV
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\nCSV gerado: {output_csv}")
        print(f"Total de links: {len(df)}")
        
        # Estatísticas
        print("\nEstatísticas por marketplace:")
        print(df['marketplace'].value_counts().to_string())
        
        return True
        
    except Exception as e:
        print(f"Erro ao processar JSON com DuckDB: {e}")
        import traceback
        traceback.print_exc()
        return False

def tratar_link_insta(link):
    """Extrai o username do link do Instagram"""
    try:
        user = str(link).split('/')[3]
        return user
    except (IndexError, AttributeError):
        return None

def extrair_usuarios_instagram(excel_path):
    """Extrai lista de usuários do Instagram das planilhas Hyeser e Fabio"""
    
    try:
        print(f"Lendo Excel: {excel_path}")
        
        # Ler planilhas
        excel_file = pd.ExcelFile(excel_path)
        print(f"Planilhas disponíveis: {excel_file.sheet_names}")
        
        df1 = pd.read_excel(excel_path, sheet_name='Hyeser')
        df2 = pd.read_excel(excel_path, sheet_name='Fabio')
        
        print(f"\nPlanilha Hyeser:")
        print(f"  Colunas: {df1.columns.tolist()}")
        print(f"  Total de linhas: {len(df1)}")
        
        print(f"\nPlanilha Fabio:")
        print(f"  Colunas: {df2.columns.tolist()}")
        print(f"  Total de linhas: {len(df2)}")
        
        # Processar aba Fabio
        df2 = df2[['LINK', 'Rede']]
        df2 = df2.query("Rede == 'Instagram'")
        print(f"  Instagram filtrado em Fabio: {len(df2)} linhas")
        
        # Concatenar dataframes
        df = pd.concat([df1, df2], ignore_index=True)
        print(f"\nTotal após concatenar: {len(df)} linhas")
        
        # Extrair usernames dos links
        df['username'] = df['LINK'].apply(tratar_link_insta)
        
        # Filtrar valores nulos
        df = df[df['username'].notna()]
        print(f"Linhas com username válido: {len(df)}")
        
        lista_usernames = list(df['username'].unique())
        
        print(f"\n✓ Total de usernames únicos: {len(lista_usernames)}")
        return lista_usernames
        
    except Exception as e:
        print(f"\nERRO ao ler Excel: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    """Função principal DEBUG"""
    print("=" * 80)
    print("INSTAGRAM STORIES CAPTURE - DEBUG MODE")
    print("Cloud Run Job com logs detalhados")
    print("=" * 80)
    print()
    
    # Configurações
    BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'instagram-stories-data')
    EXCEL_GCS_PATH = os.environ.get('EXCEL_GCS_PATH', 'excel_base_input/Perfis testes - Novembro.xlsx')
    INSTAGRAM_LOGIN = os.environ.get('INSTAGRAM_LOGIN')
    INSTAGRAM_SENHA = os.environ.get('INSTAGRAM_SENHA')
    DELAY = int(os.environ.get('DELAY', '8'))  # Aumentado para 8s
    
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Excel: {EXCEL_GCS_PATH}")
    print(f"Login: {INSTAGRAM_LOGIN}")
    print(f"Delay: {DELAY}s")
    print()
    
    if not INSTAGRAM_LOGIN or not INSTAGRAM_SENHA:
        print("ERRO: Credenciais do Instagram não configuradas!")
        return
    
    try:
        # 1. Download do Excel do GCS
        print("=" * 80)
        print("ETAPA 1: Download da base Excel")
        print("=" * 80)
        excel_temp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False).name
        
        if not download_from_gcs(BUCKET_NAME, EXCEL_GCS_PATH, excel_temp):
            print("ERRO: Não foi possível baixar o arquivo Excel")
            return
        
        # Verificar se o arquivo foi baixado corretamente
        if not os.path.exists(excel_temp):
            print(f"ERRO: Arquivo temporário não existe: {excel_temp}")
            return
        
        file_size = os.path.getsize(excel_temp)
        print(f"Arquivo baixado com sucesso: {file_size} bytes")
        
        if file_size == 0:
            print("ERRO: Arquivo Excel está vazio!")
            return
        
        # 2. Extrair usuários
        print()
        print("=" * 80)
        print("ETAPA 2: Extração de usuários")
        print("=" * 80)
        usuarios = extrair_usuarios_instagram(excel_temp)
        
        if not usuarios:
            print("ERRO: Nenhum usuário encontrado no Excel")
            return
        
        print(f"\nUsuários a processar:")
        for idx, user in enumerate(usuarios, 1):
            print(f"  {idx}. {user}")
        
        # Limpar arquivo temporário do Excel
        os.unlink(excel_temp)
        
        # 3. Capturar stories
        print()
        print("=" * 80)
        print("ETAPA 3: Captura de stories (DEBUG)")
        print("=" * 80)
        resultados = capturar_multiplas_paginas(
            lista_usuarios=usuarios,
            usuario_login=INSTAGRAM_LOGIN,
            senha_login=INSTAGRAM_SENHA,
            delay=DELAY,
            max_tentativas_login=3,
            bucket_name=BUCKET_NAME,
            gcs_folder='json_ext'
        )
        
        if not resultados:
            print("\nAVISO: Nenhum story foi capturado")
            print("Verifique os logs de debug salvos na pasta 'debug/' do bucket")
            return
        
        # 4. Processar JSONs com DuckDB
        print()
        print("=" * 80)
        print("ETAPA 4: Processamento com DuckDB")
        print("=" * 80)
        
        # Download dos JSONs do GCS
        json_temp_dir = tempfile.mkdtemp()
        json_files = []
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix='json_ext/')
        
        for blob in blobs:
            if blob.name.endswith('.json'):
                local_path = os.path.join(json_temp_dir, os.path.basename(blob.name))
                blob.download_to_filename(local_path)
                json_files.append(local_path)
        
        print(f"JSONs baixados: {len(json_files)}")
        
        if not json_files:
            print("ERRO: Nenhum JSON encontrado para processar")
            return
        
        # Gerar CSV consolidado
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_output = os.path.join(json_temp_dir, f'output_final_{timestamp}.csv')
        
        if processar_json_com_duckdb(json_files, csv_output):
            # Upload do CSV para GCS
            gcs_csv_path = f'csv_output/output_final_{timestamp}.csv'
            upload_to_gcs(BUCKET_NAME, csv_output, gcs_csv_path)
        
        # Limpar arquivos temporários
        for f in json_files:
            os.unlink(f)
        os.unlink(csv_output)
        os.rmdir(json_temp_dir)
        
        print()
        print("=" * 80)
        print("PROCESSAMENTO CONCLUÍDO!")
        print("=" * 80)
        print(f"Stories capturados: {len(resultados)}")
        print(f"CSV gerado: gs://{BUCKET_NAME}/{gcs_csv_path}")
        print(f"Logs de debug: gs://{BUCKET_NAME}/debug/")
        
    except Exception as e:
        print(f"\nERRO CRÍTICO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
