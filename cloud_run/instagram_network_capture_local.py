from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import time
import re
import os

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
        
        # Preencher usuário e senha
        username_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        username_input.send_keys(usuario)
        
        password_input = driver.find_element(By.NAME, "password")
        password_input.send_keys(senha)
        
        # Clicar em login
        login_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        login_button.click()
        time.sleep(5)
        
        # Fechar popups
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
        
        return "login" not in driver.current_url
            
    except Exception as e:
        print(f"Erro no login: {e}")
        return False

def capturar_stories_usuario(driver, username, delay=3, output_folder="."):
    """Captura o retorno do endpoint de stories para um usuário específico"""
    url = f"https://www.instagram.com/stories/{username}/"
    
    try:
        driver.get(url)
        time.sleep(8)
        
        # Capturar requisições
        logs = driver.get_log('performance')
        
        endpoint_alvo = f"{username}/?r="
        
        for log in logs:
            try:
                message = json.loads(log['message'])
                method = message.get('message', {}).get('method', '')
                
                if method == 'Network.responseReceived':
                    response = message['message']['params']['response']
                    request_url = response.get('url', '')
                    
                    if endpoint_alvo in request_url:
                        request_id = message['message']['params']['requestId']
                        
                        # Obter corpo da resposta
                        try:
                            response_body = driver.execute_cdp_cmd(
                                'Network.getResponseBody',
                                {'requestId': request_id}
                            )
                            
                            body = response_body.get('body', '')
                            
                            if response_body.get('base64Encoded', False):
                                import base64
                                body = base64.b64decode(body).decode('utf-8', errors='ignore')
                            
                            # Extrair JSON do HTML
                            json_data = extrair_json_stories(body)
                            
                            if json_data:
                                # Salvar apenas o JSON
                                filename_json = os.path.join(output_folder, f"{username}_stories.json")
                                with open(filename_json, 'w', encoding='utf-8') as f:
                                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                                
                                print(f"  ✓ {username}")
                                time.sleep(delay)
                                return json_data
                            else:
                                print(f"  ✗ {username} - JSON não encontrado")
                                return None
                            
                        except Exception as e:
                            print(f"  ✗ {username} - Erro: {e}")
                            return None
            
            except:
                continue
        
        print(f"  ✗ {username} - Endpoint não encontrado")
        return None
        
    except Exception as e:
        print(f"  ✗ {username} - Erro: {e}")
        return None


def capturar_multiplas_paginas(lista_usuarios, usuario_login, senha_login, delay=3, 
                               max_tentativas_login=3, output_folder=".", headless=False):
    """Captura stories de múltiplos usuários"""
    
    # Configurar Chrome
    chrome_options = Options()
    
    # Modo headless (sem interface) ou visual
    if headless:
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
        print(f"Iniciando... [{len(lista_usuarios)} páginas]\n")
        print(f"Modo: {'Headless (sem interface)' if headless else 'Visual (com interface)'}\n")
        
        # Tentar login com retry
        tentativa_login = 0
        login_sucesso = False
        
        while tentativa_login < max_tentativas_login and not login_sucesso:
            tentativa_login += 1
            
            if driver:
                driver.quit()
                time.sleep(5)
            
            # Detectar se está rodando no Docker (ambiente Linux) ou local (Windows)
            if os.path.exists('/usr/bin/chromedriver'):
                # Docker/Linux - usar ChromeDriver do sistema
                print("Detectado ambiente Docker/Linux - usando ChromeDriver do sistema")
                service = Service('/usr/bin/chromedriver')
                chrome_options.binary_location = '/usr/bin/google-chrome'
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Windows/Local - usar webdriver-manager
                print("Detectado ambiente Windows - usando webdriver-manager")
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            
            print(f"Login... (tentativa {tentativa_login}/{max_tentativas_login})")
            
            if fazer_login_instagram(driver, usuario_login, senha_login):
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
            resultado = capturar_stories_usuario(driver, username, delay, output_folder)
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
        if driver:
            driver.quit()
