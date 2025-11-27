import os
from dotenv import load_dotenv
import json
from instagram_network_capture import capturar_multiplas_paginas
import duckdb as db
import pandas as pd
from datetime import datetime
from google.cloud import storage
import shutil

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Conexões do DuckDB com extensão JSON
con = db.connect()
con.install_extension('json')
con.load_extension('json')

# Define a data atual
hoje = datetime.now().strftime('%Y%m%d')

# Configurações
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'projeto-meli-teste')
EXCEL_FILE = 'Perfis testes - Novembro.xlsx'
JSON_FOLDER = 'teste_json'
OUTPUT_FOLDER = 'instagram'

def tratar_link_insta(link):
    """Extrai o username do link do Instagram"""
    user = str(link).split('/')[3]
    return user

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Upload de arquivo para o GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)
        print(f"✓ Arquivo enviado para: gs://{bucket_name}/{destination_blob}")
        return True
    except Exception as e:
        print(f"✗ Erro ao enviar para GCS: {e}")
        return False

def main():
    print("=" * 60)
    print("INSTAGRAM STORIES CAPTURE")
    print("=" * 60)
    print()
    
    # 1. Criar pasta teste_json se não existir
    os.makedirs(JSON_FOLDER, exist_ok=True)
    
    # 2. Ler Excel e extrair usernames
    print("📊 Processando Excel...")
    df1 = pd.read_excel(EXCEL_FILE, sheet_name='Hyeser')
    df2 = pd.read_excel(EXCEL_FILE, sheet_name='Fabio')
    df2 = df2[['LINK', 'Rede']]
    df2 = df2.query("Rede == 'Instagram'")
    df = pd.concat([df1, df2], ignore_index=True)
    df['LINK'] = df['LINK'].apply(lambda x: tratar_link_insta(x))
    lista_usernames = list(df['LINK'])
    
    print(f"✓ {len(lista_usernames)} perfis encontrados")
    print()
    
    # 3. Capturar stories
    print(f"📸 Capturando stories...")
    print("-" * 60)
    capturar_multiplas_paginas(
        lista_usuarios=lista_usernames,
        usuario_login=str(os.getenv("LOGIN")),
        senha_login=str(os.getenv("SENHA")),
        delay=5,
        output_folder=JSON_FOLDER
    )
    print()
    
    # 4. Processar JSONs
    print("⚙️  Processando JSONs...")
    for item in os.listdir(JSON_FOLDER):
        if item.endswith('.json'):
            filepath = os.path.join(JSON_FOLDER, item)
            with open(filepath, 'r', encoding='utf-8') as f:
                teste = json.load(f)
                try:
                    novo = teste['require'][0][3][0]['__bbox']['require'][0][3][1]['__bbox']['result']['data']['xdt_api__v1__feed__reels_media']['reels_media']
                    with open(filepath, 'w', encoding='utf-8') as fw:
                        json.dump(novo, fw, ensure_ascii=False, indent=4)
                except (KeyError, IndexError) as e:
                    print(f"⚠️  Erro ao processar {item}: {e}")
    
    print(f"✓ JSONs processados")
    print()
    
    # 5. Gerar CSV com DuckDB
    print("📊 Gerando CSV...")
    path = os.path.join(JSON_FOLDER, '*.json')
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
    case
    when story_link_url = 'amzlink.to' then 'Amazon'
    when story_link_url = 'mercadolivre.com' then 'Mercado Livre'
    when story_link_url = 'mercadolivre.com.br' then 'Mercado Livre'
    when story_link_url = 'produto.mercadolivre.com.br' then 'Mercado Livre'
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
    when story_link_url = 'instagram.com' then 'Instagram'
    else null end as origin,
    current_date as date
    from pt2
    ''', [path]).df()
    
    csv_filename = f'output_final_{hoje}.csv'
    df.to_csv(csv_filename, index=False)
    
    print(f"✓ CSV gerado: {csv_filename}")
    print(f"✓ Total de usernames únicos: {df['username'].nunique()}")
    print()
    
    # 6. Upload para GCS
    print("☁️  Enviando para GCS...")
    gcs_path = f"{OUTPUT_FOLDER}/{csv_filename}"
    if upload_to_gcs(BUCKET_NAME, csv_filename, gcs_path):
        # Remover CSV local após upload
        os.remove(csv_filename)
        print(f"✓ CSV local removido")
    print()
    
    # 7. Limpar pasta teste_json
    print("🧹 Limpando pasta teste_json...")
    if os.path.exists(JSON_FOLDER):
        shutil.rmtree(JSON_FOLDER)
        print(f"✓ Pasta {JSON_FOLDER} limpa")
    print()
    
    print("=" * 60)
    print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO")
    print(f"📊 Arquivo: gs://{BUCKET_NAME}/{gcs_path}")
    print("=" * 60)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n✗ ERRO: {e}")
        import traceback
        traceback.print_exc()
