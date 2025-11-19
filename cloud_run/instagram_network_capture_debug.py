from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage
import json
import time
import re
import os
import tempfile

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

def fazer_login_instagram(driver, usuario, senha):
    """Faz login no Instagram."""
    try:
        driver.get("https://www.instagram.com/accounts/login/")
        time.sleep(3)
        
        wait = WebDriverWait(driver, 30)
        
        # Aceitar cookies
        try:
            cookies_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Permitir') or contains(text(), 'Allow') or contains(text(), 'Accept')]")
            cookies_button.click()
            time.sleep(1)
        except:
            pass
        
        # Aguardar campos de login
        username_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        password_input = driver.find_element(By.NAME, "password")
        
        # Preencher credenciais
        username_input.send_keys(usuario)
        password_input.send_keys(senha)
        
        # Clicar no botão de login
        login_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        login_button.click()
        
        # Aguardar redirecionamento
        time.sleep(5)
        
        # Verificar se o login foi bem-sucedido
        if "accounts/login" in driver.current_url:
            return False
        
        # Lidar com prompts de "Salvar informações" ou "Ativar notificações"
        try:
            not_now_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Agora não') or contains(text(), 'Not Now')]")
            not_now_button.click()
            time.sleep(1)
        except:
            pass
        
        try:
            not_now_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Agora não') or contains(text(), 'Not Now')]")
            not_now_button.click()
            time.sleep(1)
        except:
            pass
        
        return True
        
    except Exception as e:
        print(f"Erro no login: {e}")
        return False

def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    """Upload de arquivo para o Google Cloud Storage"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)
        print(f"  Uploaded to GCS: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"  Erro no upload GCS: {e}")

def capturar_stories_usuario(driver, username, delay, bucket_name=None, gcs_folder="json"):
    """Captura stories de um único usuário - VERSÃO DEBUG"""
    try:
        url = f"https://www.instagram.com/stories/{username}/"
        driver.get(url)
        
        print(f"\n=== DEBUG: {username} ===")
        print(f"URL: {url}")
        print(f"Aguardando {delay}s para carregar stories...")
        time.sleep(delay)
        
        # Obter logs de performance
        logs = driver.get_log('performance')
        print(f"Total de logs capturados: {len(logs)}")
        
        # Analisar logs em busca do endpoint
        endpoint_encontrado = False
        requests_relevantes = []
        
        for log in logs:
            try:
                log_message = json.loads(log['message'])
                message = log_message.get('message', {})
                method = message.get('method', '')
                
                # Capturar Network.responseReceived
                if method == 'Network.responseReceived':
                    params = message.get('params', {})
                    response = params.get('response', {})
                    request_url = response.get('url', '')
                    
                    # Debug: mostrar URLs relevantes
                    if 'instagram.com' in request_url and ('api' in request_url or 'graphql' in request_url):
                        requests_relevantes.append(request_url)
                    
                    if 'xdt_api__v1__feed__reels_media' in request_url:
                        endpoint_encontrado = True
                        print(f"✓ Endpoint encontrado!")
                        print(f"  URL: {request_url}")
                        
                        # Aguardar um pouco mais para garantir que o conteúdo foi carregado
                        print(f"  Aguardando mais {delay}s para garantir carregamento completo...")
                        time.sleep(delay)
                        
                        # Capturar HTML
                        html_content = driver.page_source
                        print(f"  Tamanho do HTML: {len(html_content)} bytes")
                        
                        # Extrair JSON
                        json_data = extrair_json_stories(html_content)
                        
                        if json_data:
                            # Salvar temporariamente
                            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8').name
                            with open(temp_file, 'w', encoding='utf-8') as f:
                                json.dump(json_data, f, ensure_ascii=False, indent=2)
                            
                            print(f"  JSON extraído com sucesso ({len(json.dumps(json_data))} bytes)")
                            
                            # Upload para GCS se bucket_name foi fornecido
                            if bucket_name:
                                gcs_path = f"{gcs_folder}/{username}_stories.json"
                                upload_to_gcs(bucket_name, temp_file, gcs_path)
                            
                            # Limpar arquivo temporário
                            os.unlink(temp_file)
                            
                            print(f"  ✓ {username} - SUCESSO")
                            time.sleep(delay)
                            return json_data
                        else:
                            print(f"  ✗ {username} - JSON não encontrado no HTML")
                            
                            # Salvar HTML para debug
                            if bucket_name:
                                debug_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8').name
                                with open(debug_file, 'w', encoding='utf-8') as f:
                                    f.write(html_content)
                                gcs_path = f"debug/{username}_page.html"
                                upload_to_gcs(bucket_name, debug_file, gcs_path)
                                os.unlink(debug_file)
                            
                            return None
                        
            except:
                continue
        
        # Se não encontrou o endpoint
        if not endpoint_encontrado:
            print(f"  ✗ {username} - Endpoint não encontrado")
            print(f"  Requests API/GraphQL capturados: {len(requests_relevantes)}")
            
            if requests_relevantes:
                print(f"  URLs relevantes encontradas:")
                for url in requests_relevantes[:5]:  # Mostrar apenas as 5 primeiras
                    print(f"    - {url[:100]}...")
            
            # Salvar logs completos para debug
            if bucket_name:
                debug_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8').name
                with open(debug_file, 'w', encoding='utf-8') as f:
                    json.dump(logs, f, indent=2)
                gcs_path = f"debug/{username}_logs.json"
                upload_to_gcs(bucket_name, debug_file, gcs_path)
                os.unlink(debug_file)
                print(f"  Logs salvos em: gs://{bucket_name}/{gcs_path}")
            
            return None
        
    except Exception as e:
        print(f"  ✗ {username} - Erro: {e}")
        import traceback
        traceback.print_exc()
        return None


def capturar_multiplas_paginas(lista_usuarios, usuario_login, senha_login, delay=8, 
                               max_tentativas_login=3, bucket_name=None, gcs_folder="json"):
    """Captura stories de múltiplos usuários - VERSÃO DEBUG com delay aumentado"""
    
    # Configurar Chrome (mesma config que funciona localmente)
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    driver = None
    resultados = []
    
    try:
        print(f"=== INICIANDO DEBUG ===")
        print(f"Total de usuários: {len(lista_usuarios)}")
        print(f"Delay: {delay}s\n")
        
        # Tentar login com retry
        tentativa_login = 0
        login_sucesso = False
        
        while tentativa_login < max_tentativas_login and not login_sucesso:
            tentativa_login += 1
            
            if driver:
                driver.quit()
                time.sleep(5)
            
            # Detectar se está rodando no Docker/Cloud Run (ambiente Linux) ou local (Windows)
            if os.path.exists('/usr/bin/chromedriver'):
                # Docker/Linux/Cloud Run - usar ChromeDriver do sistema
                print("Ambiente: Docker/Linux/Cloud Run")
                print("ChromeDriver: /usr/bin/chromedriver")
                service = Service('/usr/bin/chromedriver')
                chrome_options.binary_location = '/usr/bin/google-chrome'
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Windows/Local - usar webdriver-manager
                from webdriver_manager.chrome import ChromeDriverManager
                print("Ambiente: Windows/Local")
                print("ChromeDriver: webdriver-manager")
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            
            print(f"\nTentativa de login {tentativa_login}/{max_tentativas_login}...")
            
            if fazer_login_instagram(driver, usuario_login, senha_login):
                print("✓ Login realizado com sucesso\n")
                login_sucesso = True
            else:
                print(f"✗ Falha no login (tentativa {tentativa_login})")
                
                # Salvar screenshot para debug
                if bucket_name:
                    try:
                        screenshot_file = tempfile.NamedTemporaryFile(suffix='.png', delete=False).name
                        driver.save_screenshot(screenshot_file)
                        gcs_path = f"debug/login_failed_attempt_{tentativa_login}.png"
                        upload_to_gcs(bucket_name, screenshot_file, gcs_path)
                        os.unlink(screenshot_file)
                        print(f"  Screenshot salvo: gs://{bucket_name}/{gcs_path}")
                    except Exception as e:
                        print(f"  Erro ao salvar screenshot: {e}")
                
                if tentativa_login < max_tentativas_login:
                    print(f"Aguardando 10s para nova tentativa...\n")
                    time.sleep(10)
        
        if not login_sucesso:
            print("✗ FALHA: Não foi possível fazer login após todas as tentativas")
            return resultados
        
        # Processar cada usuário
        print("=== INICIANDO CAPTURA ===")
        sucesso = 0
        
        for idx, username in enumerate(lista_usuarios, 1):
            print(f"\n[{idx}/{len(lista_usuarios)}] Processando: {username}")
            resultado = capturar_stories_usuario(driver, username, delay, bucket_name, gcs_folder)
            if resultado:
                sucesso += 1
                resultados.append({"username": username, "data": resultado})
        
        # Resumo
        print(f"\n=== RESUMO ===")
        print(f"Sucesso: {sucesso}/{len(lista_usuarios)}")
        print(f"Falhas: {len(lista_usuarios) - sucesso}/{len(lista_usuarios)}")
        return resultados
        
    except Exception as e:
        print(f"\n✗ ERRO CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        return resultados
        
    finally:
        if driver:
            driver.quit()
