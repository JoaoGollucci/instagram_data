import os
import json
import duckdb as db
import pandas as pd
from datetime import datetime
from instagram_network_capture_cloudrun import capturar_multiplas_paginas
from google.cloud import storage
import tempfile
import sys

# Configurações
BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'projeto-meli-teste')
JSON_FOLDER = os.environ.get('GCS_JSON_FOLDER', 'json_ext')
CSV_FOLDER = os.environ.get('GCS_CSV_FOLDER', 'csv_output')
EXCEL_FOLDER = os.environ.get('GCS_EXCEL_FOLDER', 'excel_base_input')
EXCEL_FILENAME = os.environ.get('EXCEL_FILENAME', 'Perfis testes - Novembro.xlsx')
INSTAGRAM_LOGIN = os.environ.get('INSTAGRAM_LOGIN')
INSTAGRAM_SENHA = os.environ.get('INSTAGRAM_SENHA')
DELAY = int(os.environ.get('DELAY', '5'))

def tratar_link_insta(link):
    """Extrai o username do link do Instagram"""
    try:
        user = str(link).split('/')[3]
        return user
    except (IndexError, AttributeError):
        return None

def baixar_excel_do_gcs(bucket_name, excel_folder, excel_filename):
    """Baixa o arquivo Excel do GCS"""
    try:
        print(f"📥 Baixando Excel: gs://{bucket_name}/{excel_folder}/{excel_filename}")
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{excel_folder}/{excel_filename}")
        
        # Criar arquivo temporário
        temp_excel = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        blob.download_to_filename(temp_excel.name)
        
        print(f"✓ Excel baixado com sucesso")
        return temp_excel.name
        
    except Exception as e:
        print(f"✗ Erro ao baixar Excel do GCS: {e}")
        return None

def processar_excel_e_extrair_usernames(excel_path):
    """Processa o Excel e extrai a lista de usernames"""
    try:
        print("📊 Processando Excel...")
        
        # Ler as duas abas
        df1 = pd.read_excel(excel_path, sheet_name='Hyeser')
        df2 = pd.read_excel(excel_path, sheet_name='Fabio')
        
        # Processar aba Fabio
        df2 = df2[['LINK', 'Rede']]
        df2 = df2.query("Rede == 'Instagram'")
        
        # Concatenar dataframes
        df = pd.concat([df1, df2], ignore_index=True)
        
        # Extrair usernames dos links
        df['username'] = df['LINK'].apply(tratar_link_insta)
        
        # Filtrar valores nulos
        df = df[df['username'].notna()]
        
        lista_usernames = list(df['username'].unique())
        
        print(f"✓ {len(lista_usernames)} usernames únicos extraídos")
        
        return lista_usernames
        
    except Exception as e:
        print(f"✗ Erro ao processar Excel: {e}")
        import traceback
        traceback.print_exc()
        return []

def processar_jsons_e_gerar_csv(bucket_name, json_folder, csv_folder):
    """Processa os JSONs do GCS e gera o CSV final"""
    try:
        hoje = datetime.now().strftime('%Y%m%d')
        
        # Baixar todos os JSONs do bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=json_folder)
        
        # Criar diretório temporário para os JSONs
        temp_dir = tempfile.mkdtemp()
        json_temp_dir = os.path.join(temp_dir, 'json')
        os.makedirs(json_temp_dir, exist_ok=True)
        
        # Baixar JSONs
        json_count = 0
        for blob in blobs:
            if blob.name.endswith('.json'):
                local_path = os.path.join(json_temp_dir, os.path.basename(blob.name))
                blob.download_to_filename(local_path)
                json_count += 1
        
        print(f"✓ {json_count} arquivos JSON baixados do GCS")
        
        # Processar JSONs com DuckDB
        con = db.connect()
        con.install_extension('json')
        con.load_extension('json')
        
        # Processar os arquivos conforme o notebook
        for item in os.listdir(json_temp_dir):
            if item.endswith('.json'):
                filepath = os.path.join(json_temp_dir, item)
                with open(filepath, 'r', encoding='utf-8') as f:
                    teste = json.load(f)
                    try:
                        novo = teste['require'][0][3][0]['__bbox']['require'][0][3][1]['__bbox']['result']['data']['xdt_api__v1__feed__reels_media']['reels_media']
                        with open(filepath, 'w', encoding='utf-8') as fw:
                            json.dump(novo, fw, ensure_ascii=False, indent=4)
                    except (KeyError, IndexError) as e:
                        print(f"⚠️  Erro ao processar {item}: {e}")
        
        # Executar query DuckDB
        path = os.path.join(json_temp_dir, '*.json')
        df = con.execute('''
        with pt1 as (
        SELECT
        user.username AS username,
        unnest(items) AS item
        FROM read_json_auto(?, ignore_errors=true)
        ),
        pt2 as (
        SELECT
        username,
        replace(split(split(unnest(item.story_link_stickers).story_link.url, 'u=')[2], '%2F')[3], 'www.', '') AS story_link_url
        FROM pt1
        )
        select
        username,
        story_link_url,
        case
        when story_link_url = 'amzlink.to' then 'Amazon'
        when story_link_url = 'mercadolivre.com' then 'Mercado Livre'
        when story_link_url = 's.shopee.com.br' then 'Shopee'
        when story_link_url = 'minhaloja.natura.com' then 'Natura'
        when story_link_url = 'magazinevoce.com.br' then 'Magazine Luiza'
        when story_link_url = 'elausa.com.br' then 'Ela Usa'
        when story_link_url = 'epocacosmeticos.com.br' then 'Época Cosméticos'
        when story_link_url = 'natura.com.br' then 'Natura'
        when story_link_url = 'sminhaloja.natura.com' then 'Natura'
        when story_link_url = 'api.whatsapp.com' then 'WhatsApp'
        when story_link_url = 'google.com' then 'Google'
        when story_link_url = 'encurtador.com.br' then 'Encurtador'
        when story_link_url = 'tinyurl.com' then 'Encurtador'
        when story_link_url = 'br.shp.ee' then 'Shopee'
        else null end as origin
        from pt2
        ''', [path]).df()
        
        # Salvar CSV localmente
        csv_filename = f'output_final_{hoje}.csv'
        csv_temp_path = os.path.join(temp_dir, csv_filename)
        df.to_csv(csv_temp_path, index=False)
        
        print(f"✓ CSV gerado: {csv_filename}")
        print(f"✓ Total de usernames únicos: {df['username'].nunique()}")
        
        # Upload do CSV para GCS
        blob = bucket.blob(f"{csv_folder}/{csv_filename}")
        blob.upload_from_filename(csv_temp_path)
        
        print(f"✓ CSV enviado para: gs://{bucket_name}/{csv_folder}/{csv_filename}")
        
        # Limpar arquivos temporários
        import shutil
        shutil.rmtree(temp_dir)
        
        return {
            "status": "success",
            "csv_file": f"gs://{bucket_name}/{csv_folder}/{csv_filename}",
            "total_usernames": int(df['username'].nunique()),
            "total_rows": len(df)
        }
        
    except Exception as e:
        print(f"✗ Erro ao processar JSONs: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e)
        }

def main():
    """Função principal para executar o job"""
    print("=" * 60)
    print("🚀 INSTAGRAM DATA CAPTURE - Cloud Run Job")
    print("=" * 60)
    print()
    
    # Validar configurações
    if not INSTAGRAM_LOGIN or not INSTAGRAM_SENHA:
        print("✗ ERRO: Credenciais do Instagram não configuradas")
        print("  Configure INSTAGRAM_LOGIN e INSTAGRAM_SENHA")
        sys.exit(1)
    
    if not BUCKET_NAME or BUCKET_NAME == 'your-bucket-name':
        print("✗ ERRO: GCS_BUCKET_NAME não configurado")
        sys.exit(1)
    
    print(f"📦 Bucket GCS: {BUCKET_NAME}")
    print(f"📂 Pasta JSON: {JSON_FOLDER}")
    print(f"📂 Pasta CSV: {CSV_FOLDER}")
    print(f"📂 Pasta Excel: {EXCEL_FOLDER}")
    print(f"📄 Arquivo Excel: {EXCEL_FILENAME}")
    print(f"⏱️  Delay: {DELAY}s")
    print()
    
    # Baixar Excel do GCS
    excel_path = baixar_excel_do_gcs(BUCKET_NAME, EXCEL_FOLDER, EXCEL_FILENAME)
    if not excel_path:
        print("✗ ERRO: Não foi possível baixar o Excel")
        sys.exit(1)
    
    # Processar Excel e extrair usernames
    lista_usernames = processar_excel_e_extrair_usernames(excel_path)
    if not lista_usernames:
        print("✗ ERRO: Nenhum username encontrado no Excel")
        os.unlink(excel_path)
        sys.exit(1)
    
    # Limpar arquivo temporário do Excel
    os.unlink(excel_path)
    
    print()
    print(f"📊 Iniciando captura de {len(lista_usernames)} perfis")
    print("-" * 60)
    print()
    
    # Capturar dados dos stories
    resultados = capturar_multiplas_paginas(
        lista_usuarios=lista_usernames,
        usuario_login=INSTAGRAM_LOGIN,
        senha_login=INSTAGRAM_SENHA,
        delay=DELAY,
        bucket_name=BUCKET_NAME,
        gcs_folder=JSON_FOLDER
    )
    
    print()
    print("-" * 60)
    print(f"✓ Captura concluída: {len(resultados)}/{len(lista_usernames)} perfis")
    print()
    
    # Processar JSONs e gerar CSV
    print("📊 Gerando CSV consolidado...")
    print("-" * 60)
    print()
    
    resultado_csv = processar_jsons_e_gerar_csv(BUCKET_NAME, JSON_FOLDER, CSV_FOLDER)
    
    print()
    print("=" * 60)
    if resultado_csv.get("status") == "success":
        print("✅ JOB CONCLUÍDO COM SUCESSO")
        print()
        print(f"📊 Resultados:")
        print(f"   - Perfis processados: {len(resultados)}/{len(lista_usernames)}")
        print(f"   - Usernames únicos: {resultado_csv.get('total_usernames', 0)}")
        print(f"   - Total de linhas CSV: {resultado_csv.get('total_rows', 0)}")
        print(f"   - Arquivo CSV: {resultado_csv.get('csv_file', 'N/A')}")
    else:
        print("❌ JOB CONCLUÍDO COM ERROS")
        print()
        print(f"   Erro: {resultado_csv.get('message', 'Erro desconhecido')}")
    print("=" * 60)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Job interrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n✗ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
