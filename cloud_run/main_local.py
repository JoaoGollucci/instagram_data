import os
import json
import duckdb as db
import pandas as pd
from datetime import datetime
from instagram_network_capture_local import capturar_multiplas_paginas
# from google.cloud import storage
import tempfile
import sys

# Configurações para teste local
BUCKET_NAME = 'projeto-meli-teste'  # Não usado localmente
JSON_FOLDER = 'json_local'
CSV_FOLDER = 'csv_local'
# Caminho do Excel - funciona local e no Docker
EXCEL_PATH = os.environ.get('EXCEL_PATH', r'C:/Users/suporte/Desktop/instagram_data/Perfis testes - Novembro.xlsx')
INSTAGRAM_LOGIN = os.environ.get('INSTAGRAM_LOGIN', 'testdevjg')
INSTAGRAM_SENHA = os.environ.get('INSTAGRAM_SENHA', 'Evalleiford10')
DELAY = int(os.environ.get('DELAY', '5'))
HEADLESS = os.environ.get('HEADLESS', 'False').lower() == 'true'  # False = interface visual (padrão local)

def tratar_link_insta(link):
    """Extrai o username do link do Instagram"""
    try:
        user = str(link).split('/')[3]
        return user
    except (IndexError, AttributeError):
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

def processar_jsons_e_gerar_csv(json_folder, csv_folder):
    """Processa os JSONs locais e gera o CSV final"""
    try:
        hoje = datetime.now().strftime('%Y%m%d')
        
        # Criar diretórios se não existirem
        os.makedirs(json_folder, exist_ok=True)
        os.makedirs(csv_folder, exist_ok=True)
        
        # Verificar se há JSONs
        json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
        print(f"✓ {len(json_files)} arquivos JSON encontrados")
        
        if len(json_files) == 0:
            print("⚠️  Nenhum arquivo JSON para processar")
            return {
                "status": "error",
                "message": "Nenhum arquivo JSON encontrado"
            }
        
        # Processar JSONs com DuckDB
        con = db.connect()
        con.install_extension('json')
        con.load_extension('json')
        
        # Processar os arquivos conforme o notebook
        for item in json_files:
            filepath = os.path.join(json_folder, item)
            with open(filepath, 'r', encoding='utf-8') as f:
                teste = json.load(f)
                try:
                    novo = teste['require'][0][3][0]['__bbox']['require'][0][3][1]['__bbox']['result']['data']['xdt_api__v1__feed__reels_media']['reels_media']
                    with open(filepath, 'w', encoding='utf-8') as fw:
                        json.dump(novo, fw, ensure_ascii=False, indent=4)
                except (KeyError, IndexError) as e:
                    print(f"⚠️  Erro ao processar {item}: {e}")
        
        # Executar query DuckDB
        path = os.path.join(json_folder, '*.json')
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
        csv_path = os.path.join(csv_folder, csv_filename)
        df.to_csv(csv_path, index=False)
        
        print(f"✓ CSV gerado: {csv_path}")
        print(f"✓ Total de usernames únicos: {df['username'].nunique()}")
        
        return {
            "status": "success",
            "csv_file": csv_path,
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
    print("🚀 INSTAGRAM DATA CAPTURE - Teste Local")
    print("=" * 60)
    print()
    
    # Validar configurações
    if not INSTAGRAM_LOGIN or INSTAGRAM_LOGIN == 'seu_usuario':
        print("✗ ERRO: Configure INSTAGRAM_LOGIN e INSTAGRAM_SENHA")
        print("  Edite o arquivo main_local.py ou use variáveis de ambiente")
        sys.exit(1)
    
    print(f"📄 Arquivo Excel: {EXCEL_PATH}")
    print(f"📂 Pasta JSON: {JSON_FOLDER}")
    print(f"📂 Pasta CSV: {CSV_FOLDER}")
    print(f"⏱️  Delay: {DELAY}s")
    print(f"🖥️  Modo Headless: {HEADLESS} (Visual: {not HEADLESS})")
    print()
    
    # Verificar se Excel existe
    if not os.path.exists(EXCEL_PATH):
        print(f"✗ ERRO: Excel não encontrado: {EXCEL_PATH}")
        sys.exit(1)
    
    # Processar Excel e extrair usernames
    lista_usernames = processar_excel_e_extrair_usernames(EXCEL_PATH)
    if not lista_usernames:
        print("✗ ERRO: Nenhum username encontrado no Excel")
        sys.exit(1)
    
    print()
    print(f"📊 Iniciando captura de {len(lista_usernames)} perfis")
    print("-" * 60)
    print()
    
    # Criar pasta para JSONs
    os.makedirs(JSON_FOLDER, exist_ok=True)
    
    # Capturar dados dos stories
    resultados = capturar_multiplas_paginas(
        lista_usuarios=['sensacional', 'motivei'],
        usuario_login=INSTAGRAM_LOGIN,
        senha_login=INSTAGRAM_SENHA,
        delay=DELAY,
        output_folder=JSON_FOLDER,
        headless=HEADLESS
    )
    
    print()
    print("-" * 60)
    print(f"✓ Captura concluída: {len(resultados)}/{len(lista_usernames)} perfis")
    print()
    
    # # Processar JSONs e gerar CSV
    # print("📊 Gerando CSV consolidado...")
    # print("-" * 60)
    # print()
    
    # resultado_csv = processar_jsons_e_gerar_csv(JSON_FOLDER, CSV_FOLDER)
    
    # print()
    # print("=" * 60)
    # if resultado_csv.get("status") == "success":
    #     print("✅ TESTE CONCLUÍDO COM SUCESSO")
    #     print()
    #     print(f"📊 Resultados:")
    #     print(f"   - Perfis processados: {len(resultados)}/{len(lista_usernames)}")
    #     print(f"   - Usernames únicos: {resultado_csv.get('total_usernames', 0)}")
    #     print(f"   - Total de linhas CSV: {resultado_csv.get('total_rows', 0)}")
    #     print(f"   - Arquivo CSV: {resultado_csv.get('csv_file', 'N/A')}")
    # else:
    #     print("❌ TESTE CONCLUÍDO COM ERROS")
    #     print()
    #     print(f"   Erro: {resultado_csv.get('message', 'Erro desconhecido')}")
    # print("=" * 60)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Teste interrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n✗ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
