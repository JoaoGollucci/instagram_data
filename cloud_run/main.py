"""
Instagram Data Capture - Cloud Run Job
Versão otimizada e final para execução no Google Cloud Run

Este script:
1. Baixa um arquivo Excel do GCS com lista de perfis
2. Captura stories dos perfis usando Playwright
3. Processa os JSONs capturados
4. Gera CSV consolidado e envia para GCS
"""

import os
import sys
import json
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Optional

import duckdb as db
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

from instagram_network_capture import capturar_multiplas_paginas

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configurações do ambiente
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'projeto-meli-teste')
JSON_FOLDER = os.getenv('GCS_JSON_FOLDER', 'json_ext')
CSV_FOLDER = os.getenv('GCS_CSV_FOLDER', 'csv_output')
EXCEL_FOLDER = os.getenv('GCS_EXCEL_FOLDER', 'excel_base_input')
EXCEL_FILENAME = os.getenv('EXCEL_FILENAME', 'Perfis testes - Novembro.xlsx')
INSTAGRAM_LOGIN = os.getenv('INSTAGRAM_LOGIN')
INSTAGRAM_SENHA = os.getenv('INSTAGRAM_SENHA')
DELAY = int(os.getenv('DELAY', '5'))
MAX_TENTATIVAS_LOGIN = int(os.getenv('MAX_TENTATIVAS_LOGIN', '3'))


def tratar_link_insta(link: str) -> Optional[str]:
    """
    Extrai o username do link do Instagram
    
    Args:
        link: URL do perfil do Instagram
        
    Returns:
        Username extraído ou None se inválido
    """
    try:
        parts = str(link).split('/')
        if len(parts) > 3:
            return parts[3]
        return None
    except (IndexError, AttributeError):
        return None


def baixar_excel_do_gcs(bucket_name: str, excel_folder: str, excel_filename: str) -> Optional[str]:
    """
    Baixa o arquivo Excel do Google Cloud Storage
    
    Args:
        bucket_name: Nome do bucket GCS
        excel_folder: Pasta no GCS onde está o Excel
        excel_filename: Nome do arquivo Excel
        
    Returns:
        Caminho do arquivo temporário baixado ou None se falhar
    """
    try:
        gcs_path = f"{excel_folder}/{excel_filename}"
        logger.info(f"📥 Baixando Excel: gs://{bucket_name}/{gcs_path}")
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        if not blob.exists():
            logger.error(f"✗ Arquivo não encontrado no GCS: gs://{bucket_name}/{gcs_path}")
            return None
        
        # Criar arquivo temporário
        temp_excel = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        blob.download_to_filename(temp_excel.name)
        
        file_size = os.path.getsize(temp_excel.name)
        logger.info(f"✓ Excel baixado com sucesso ({file_size} bytes)")
        
        return temp_excel.name
        
    except Exception as e:
        logger.error(f"✗ Erro ao baixar Excel do GCS: {e}", exc_info=True)
        return None


def processar_excel_e_extrair_usernames(excel_path: str) -> List[str]:
    """
    Processa o arquivo Excel e extrai lista de usernames únicos
    
    Args:
        excel_path: Caminho do arquivo Excel
        
    Returns:
        Lista de usernames únicos
    """
    try:
        logger.info("📊 Processando Excel...")
        
        # Ler as duas abas
        df1 = pd.read_excel(excel_path, sheet_name='Hyeser')
        df2 = pd.read_excel(excel_path, sheet_name='Fabio')
        
        logger.info(f"  Aba 'Hyeser': {len(df1)} linhas")
        logger.info(f"  Aba 'Fabio': {len(df2)} linhas")
        
        # Processar aba Fabio
        df2 = df2[['LINK', 'Rede']]
        df2 = df2.query("Rede == 'Instagram'")
        
        # Concatenar dataframes
        df = pd.concat([df1, df2], ignore_index=True)
        
        # Extrair usernames dos links
        df['username'] = df['LINK'].apply(tratar_link_insta)
        
        # Filtrar valores nulos e obter únicos
        df = df[df['username'].notna()]
        lista_usernames = list(df['username'].unique())
        
        logger.info(f"✓ {len(lista_usernames)} usernames únicos extraídos")
        
        return lista_usernames
        
    except Exception as e:
        logger.error(f"✗ Erro ao processar Excel: {e}", exc_info=True)
        return []


def processar_jsons_e_gerar_csv(bucket_name: str, json_folder: str, csv_folder: str) -> Dict:
    """
    Processa os JSONs do GCS e gera CSV consolidado
    
    Args:
        bucket_name: Nome do bucket GCS
        json_folder: Pasta no GCS com JSONs
        csv_folder: Pasta no GCS para salvar CSV
        
    Returns:
        Dict com status e informações do processamento
    """
    try:
        hoje = datetime.now().strftime('%Y%m%d')
        
        logger.info("📊 Processando JSONs do GCS...")
        
        # Baixar todos os JSONs do bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=json_folder))
        
        # Filtrar apenas JSONs
        json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]
        
        if not json_blobs:
            logger.warning("⚠️  Nenhum arquivo JSON encontrado no GCS")
            return {
                "status": "error",
                "message": "Nenhum arquivo JSON encontrado"
            }
        
        logger.info(f"✓ {len(json_blobs)} arquivos JSON encontrados")
        
        # Criar diretório temporário para os JSONs
        temp_dir = tempfile.mkdtemp()
        json_temp_dir = os.path.join(temp_dir, 'json')
        os.makedirs(json_temp_dir, exist_ok=True)
        
        # Baixar JSONs
        logger.info("⬇️  Baixando JSONs...")
        for blob in json_blobs:
            local_path = os.path.join(json_temp_dir, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
        
        logger.info(f"✓ {len(json_blobs)} JSONs baixados")
        
        # Processar JSONs - extrair dados aninhados
        logger.info("🔄 Processando estrutura dos JSONs...")
        json_processados = 0
        
        for item in os.listdir(json_temp_dir):
            if item.endswith('.json'):
                filepath = os.path.join(json_temp_dir, item)
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Navegar na estrutura aninhada para extrair reels_media
                    novo = data['require'][0][3][0]['__bbox']['require'][0][3][1]['__bbox']['result']['data']['xdt_api__v1__feed__reels_media']['reels_media']
                    
                    # Sobrescrever arquivo com dados extraídos
                    with open(filepath, 'w', encoding='utf-8') as fw:
                        json.dump(novo, fw, ensure_ascii=False, indent=4)
                    
                    json_processados += 1
                    
                except (KeyError, IndexError, TypeError) as e:
                    logger.warning(f"⚠️  Erro ao processar {item}: {e}")
                    # Manter arquivo original se falhar
        
        logger.info(f"✓ {json_processados} JSONs processados com sucesso")
        
        # Processar com DuckDB
        logger.info("🦆 Processando dados com DuckDB...")
        
        con = db.connect()
        con.install_extension('json')
        con.load_extension('json')
        
        # Query SQL para extrair dados
        path = os.path.join(json_temp_dir, '*.json')
        
        df = con.execute('''
        WITH pt1 AS (
            SELECT
                user.username AS username,
                UNNEST(items) AS item
            FROM read_json_auto(?, ignore_errors=true)
        ),
        pt2 AS (
            SELECT
                username,
                REPLACE(
                    SPLIT(
                        SPLIT(
                            UNNEST(item.story_link_stickers).story_link.url, 
                            'u='
                        )[2], 
                        '%2F'
                    )[3], 
                    'www.', 
                    ''
                ) AS story_link_url
            FROM pt1
        )
        SELECT
            username,
            story_link_url,
            CASE
                WHEN story_link_url = 'amzlink.to' THEN 'Amazon'
                WHEN story_link_url = 'mercadolivre.com' THEN 'Mercado Livre'
                WHEN story_link_url = 's.shopee.com.br' THEN 'Shopee'
                WHEN story_link_url = 'minhaloja.natura.com' THEN 'Natura'
                WHEN story_link_url = 'magazinevoce.com.br' THEN 'Magazine Luiza'
                WHEN story_link_url = 'elausa.com.br' THEN 'Ela Usa'
                WHEN story_link_url = 'epocacosmeticos.com.br' THEN 'Época Cosméticos'
                WHEN story_link_url = 'natura.com.br' THEN 'Natura'
                WHEN story_link_url = 'sminhaloja.natura.com' THEN 'Natura'
                WHEN story_link_url = 'api.whatsapp.com' THEN 'WhatsApp'
                WHEN story_link_url = 'google.com' THEN 'Google'
                WHEN story_link_url = 'encurtador.com.br' THEN 'Encurtador'
                WHEN story_link_url = 'tinyurl.com' THEN 'Encurtador'
                WHEN story_link_url = 'br.shp.ee' THEN 'Shopee'
                ELSE NULL
            END AS origin
        FROM pt2
        WHERE story_link_url IS NOT NULL
        ''', [path]).df()
        
        logger.info(f"✓ Dados processados: {len(df)} linhas")
        logger.info(f"✓ Usernames únicos: {df['username'].nunique()}")
        
        # Salvar CSV localmente
        csv_filename = f'output_final_{hoje}.csv'
        csv_temp_path = os.path.join(temp_dir, csv_filename)
        df.to_csv(csv_temp_path, index=False, encoding='utf-8')
        
        logger.info(f"✓ CSV gerado: {csv_filename}")
        
        # Upload do CSV para GCS
        logger.info("⬆️  Enviando CSV para GCS...")
        blob = bucket.blob(f"{csv_folder}/{csv_filename}")
        blob.upload_from_filename(csv_temp_path)
        
        gcs_csv_path = f"gs://{bucket_name}/{csv_folder}/{csv_filename}"
        logger.info(f"✓ CSV enviado: {gcs_csv_path}")
        
        # Limpar arquivos temporários
        import shutil
        shutil.rmtree(temp_dir)
        logger.info("✓ Arquivos temporários limpos")
        
        return {
            "status": "success",
            "csv_file": gcs_csv_path,
            "total_usernames": int(df['username'].nunique()),
            "total_rows": len(df),
            "json_processados": json_processados
        }
        
    except Exception as e:
        logger.error(f"✗ Erro ao processar JSONs: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e)
        }


def main():
    """Função principal para executar o job no Cloud Run"""
    
    logger.info("=" * 70)
    logger.info("🚀 INSTAGRAM DATA CAPTURE - Cloud Run Job")
    logger.info("=" * 70)
    logger.info("")
    
    # Validar configurações obrigatórias
    if not INSTAGRAM_LOGIN or not INSTAGRAM_SENHA:
        logger.error("✗ ERRO: Credenciais do Instagram não configuradas")
        logger.error("  Configure INSTAGRAM_LOGIN e INSTAGRAM_SENHA nas variáveis de ambiente")
        sys.exit(1)
    
    if not BUCKET_NAME:
        logger.error("✗ ERRO: GCS_BUCKET_NAME não configurado")
        sys.exit(1)
    
    # Exibir configurações
    logger.info(f"📦 Bucket GCS: {BUCKET_NAME}")
    logger.info(f"📂 Pasta JSON: {JSON_FOLDER}")
    logger.info(f"📂 Pasta CSV: {CSV_FOLDER}")
    logger.info(f"📂 Pasta Excel: {EXCEL_FOLDER}")
    logger.info(f"📄 Arquivo Excel: {EXCEL_FILENAME}")
    logger.info(f"⏱️  Delay entre requisições: {DELAY}s")
    logger.info(f"🔄 Tentativas de login: {MAX_TENTATIVAS_LOGIN}")
    logger.info(f"🎭 Engine: Playwright (Chromium)")
    logger.info("")
    
    # Passo 1: Baixar Excel do GCS
    logger.info("=" * 70)
    logger.info("PASSO 1: Baixar base de dados")
    logger.info("=" * 70)
    
    excel_path = baixar_excel_do_gcs(BUCKET_NAME, EXCEL_FOLDER, EXCEL_FILENAME)
    if not excel_path:
        logger.error("✗ ERRO: Não foi possível baixar o Excel")
        sys.exit(1)
    
    logger.info("")
    
    # Passo 2: Processar Excel e extrair usernames
    logger.info("=" * 70)
    logger.info("PASSO 2: Extrair lista de perfis")
    logger.info("=" * 70)
    
    lista_usernames = processar_excel_e_extrair_usernames(excel_path)
    
    # Limpar arquivo temporário do Excel
    os.unlink(excel_path)
    
    if not lista_usernames:
        logger.error("✗ ERRO: Nenhum username encontrado no Excel")
        sys.exit(1)
    
    logger.info("")
    
    # Passo 3: Capturar dados dos stories
    logger.info("=" * 70)
    logger.info(f"PASSO 3: Capturar stories de {len(lista_usernames)} perfis")
    logger.info("=" * 70)
    logger.info("")
    
    resultados = capturar_multiplas_paginas(
        lista_usuarios=lista_usernames,
        usuario_login=INSTAGRAM_LOGIN,
        senha_login=INSTAGRAM_SENHA,
        delay=DELAY,
        max_tentativas_login=MAX_TENTATIVAS_LOGIN,
        bucket_name=BUCKET_NAME,
        gcs_folder=JSON_FOLDER
    )
    
    sucessos = sum(1 for r in resultados if r.get('status') == 'success')
    falhas = len(resultados) - sucessos
    
    logger.info("")
    logger.info(f"✓ Captura concluída: {sucessos} sucessos, {falhas} falhas")
    logger.info("")
    
    # Passo 4: Processar JSONs e gerar CSV
    logger.info("=" * 70)
    logger.info("PASSO 4: Processar dados e gerar CSV")
    logger.info("=" * 70)
    logger.info("")
    
    resultado_csv = processar_jsons_e_gerar_csv(BUCKET_NAME, JSON_FOLDER, CSV_FOLDER)
    
    logger.info("")
    
    # Resultado final
    logger.info("=" * 70)
    if resultado_csv.get("status") == "success":
        logger.info("✅ JOB CONCLUÍDO COM SUCESSO")
        logger.info("")
        logger.info(f"📊 Resumo:")
        logger.info(f"   • Perfis processados: {sucessos}/{len(lista_usernames)}")
        logger.info(f"   • JSONs processados: {resultado_csv.get('json_processados', 0)}")
        logger.info(f"   • Usernames únicos no CSV: {resultado_csv.get('total_usernames', 0)}")
        logger.info(f"   • Total de linhas no CSV: {resultado_csv.get('total_rows', 0)}")
        logger.info(f"   • Arquivo CSV: {resultado_csv.get('csv_file', 'N/A')}")
    else:
        logger.error("❌ JOB CONCLUÍDO COM ERROS")
        logger.error("")
        logger.error(f"   Erro: {resultado_csv.get('message', 'Erro desconhecido')}")
        sys.exit(1)
    
    logger.info("=" * 70)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Job interrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\n✗ ERRO FATAL: {e}", exc_info=True)
        sys.exit(1)
