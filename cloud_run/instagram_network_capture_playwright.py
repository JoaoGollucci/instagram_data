from playwright.sync_api import sync_playwright
import json
import time
import re
import os
import tempfile
from google.cloud import storage

def extrair_json_stories(html_content):
    """Extrai o JSON com dados dos stories do HTML"""
    try:
        # Procurar por todos os script tags com data-sjs
        pattern = r'<script type="application/json"[^>]*data-sjs[^>]*>(.*?)</script>'
        matches = re.findall(pattern, html_content, re.DOTALL)
        
        for script_content in matches:
            if 'xdt_api__v1__feed__reels_media' in script_content:
                # Parse o JSON
                data = json.loads(script_content)
                
                # Extrair apenas a lista "require"
                if 'require' in data:
                    # Salvar o require completo
                    return {"require": data['require']}
        
        return None
    except Exception as e:
        print(f"Erro ao extrair JSON: {e}")
        import traceback
        traceback.print_exc()
        return None

def fazer_login_instagram(page, usuario, senha):
    """Faz login no Instagram usando Playwright"""
    try:
        page.goto("https://www.instagram.com/accounts/login/")
        page.wait_for_timeout(3000)
        
        # Aceitar cookies
        try:
            page.click("button:has-text('Permitir'), button:has-text('Allow'), button:has-text('Accept')", timeout=3000)
            page.wait_for_timeout(1000)
        except:
            pass
        
        # Aguardar campos de login
        page.wait_for_selector("input[name='username']", timeout=30000)
        
        # Preencher credenciais
        page.fill("input[name='username']", usuario)
        page.fill("input[name='password']", senha)
        
        # Clicar no botão de login
        page.click("button[type='submit']")
        
        # Aguardar redirecionamento
        page.wait_for_timeout(5000)
        
        # Verificar se o login foi bem-sucedido
        if "accounts/login" in page.url:
            return False
        
        # Lidar com prompts de "Salvar informações" ou "Ativar notificações"
        try:
            page.click("button:has-text('Agora não'), button:has-text('Not Now')", timeout=3000)
            page.wait_for_timeout(1000)
        except:
            pass
        
        try:
            page.click("button:has-text('Agora não'), button:has-text('Not Now')", timeout=3000)
            page.wait_for_timeout(1000)
        except:
            pass
        
        return True
        
    except Exception as e:
        print(f"Erro no login: {e}")
        return False

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Faz upload de um arquivo para o GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(source_file_path)
        print(f"  ✓ Upload para GCS: gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"  ✗ Erro no upload para GCS: {e}")
        return False

def capturar_stories_usuario(page, username, delay=3, bucket_name=None, gcs_folder="json"):
    """Captura o retorno do endpoint de stories para um usuário específico"""
    url = f"https://www.instagram.com/stories/{username}/"
    
    try:
        page.goto(url, wait_until='networkidle')
        
        # Aguardar delay para carregar stories
        page.wait_for_timeout(delay * 1000)
        
        # Aguardar mais um pouco para garantir carregamento completo
        page.wait_for_timeout(delay * 1000)
        
        # Capturar HTML
        html_content = page.content()
        
        # Verificar se o endpoint foi chamado (checando se há dados no HTML)
        if 'xdt_api__v1__feed__reels_media' in html_content:
            # Extrair JSON
            json_data = extrair_json_stories(html_content)
            
            if json_data:
                # Criar arquivo temporário
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                    temp_file = f.name
                
                # Upload para GCS se bucket_name foi fornecido
                if bucket_name:
                    gcs_path = f"{gcs_folder}/{username}_stories.json"
                    upload_to_gcs(bucket_name, temp_file, gcs_path)
                
                # Limpar arquivo temporário
                os.unlink(temp_file)
                
                print(f"  ✓ {username}")
                page.wait_for_timeout(delay * 1000)
                return json_data
            else:
                print(f"  ✗ {username} - JSON não encontrado no HTML")
                return None
        else:
            print(f"  ✗ {username} - Endpoint não encontrado")
            return None
        
    except Exception as e:
        print(f"  ✗ {username} - Erro: {e}")
        import traceback
        traceback.print_exc()
        return None

def capturar_multiplas_paginas(lista_usuarios, usuario_login, senha_login, delay=5, 
                               max_tentativas_login=3, bucket_name=None, gcs_folder="json"):
    """Captura stories de múltiplos usuários e salva no GCS usando Playwright"""
    
    resultados = []
    
    with sync_playwright() as p:
        # Configurar browser
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        
        # Criar contexto com user agent
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        )
        
        page = context.new_page()
        
        try:
            print(f"Iniciando... [{len(lista_usuarios)} páginas]\n")
            
            # Tentar login com retry
            tentativa_login = 0
            login_sucesso = False
            
            while tentativa_login < max_tentativas_login and not login_sucesso:
                tentativa_login += 1
                
                print(f"Login... (tentativa {tentativa_login}/{max_tentativas_login})")
                
                if fazer_login_instagram(page, usuario_login, senha_login):
                    print("✓ Login realizado\n")
                    login_sucesso = True
                else:
                    print(f"✗ Erro no login (tentativa {tentativa_login})")
                    if tentativa_login < max_tentativas_login:
                        print(f"Aguardando 10s para nova tentativa...\n")
                        time.sleep(10)
            
            if not login_sucesso:
                print("✗ Falha após todas as tentativas de login")
                return resultados
            
            # Processar cada usuário
            print("Capturando:")
            sucesso = 0
            
            for username in lista_usuarios:
                resultado = capturar_stories_usuario(page, username, delay, bucket_name, gcs_folder)
                if resultado:
                    sucesso += 1
                    resultados.append({"username": username, "data": resultado})
            
            # Resumo
            print(f"\nConcluído: {sucesso}/{len(lista_usuarios)}")
            return resultados
            
        except Exception as e:
            print(f"✗ Erro: {e}")
            return resultados
            
        finally:
            context.close()
            browser.close()
