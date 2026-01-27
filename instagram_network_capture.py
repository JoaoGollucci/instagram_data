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
        
        if not matches:
            return None
        
        for script_content in matches:
            if 'xdt_api__v1__feed__reels_media' in script_content:
                # Parse o JSON
                data = json.loads(script_content)
                
                # Extrair apenas a lista "require"
                if 'require' in data:
                    return {"require": data['require']}
        
        return None
    except json.JSONDecodeError:
        return None
    except Exception:
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
        
        # Preencher usuário - tenta padrão original, depois padrão novo (teste A/B)
        username_input = None
        password_input = None
        login_pattern = None  # Para identificar qual padrão foi usado
        
        # Padrão 1: name="username" (original)
        try:
            username_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
            login_pattern = "original"
            print("  → Detectado padrão de login: ORIGINAL (username/password)")
        except:
            pass
        
        # Padrão 2: name="email" (novo teste A/B do Instagram)
        if username_input is None:
            try:
                username_input = wait.until(EC.presence_of_element_located((By.NAME, "email")))
                login_pattern = "novo_ab"
                print("  → Detectado padrão de login: NOVO A/B (email/pass)")
            except:
                pass
        
        # Fallback: busca por atributos alternativos
        if username_input is None:
            try:
                # Tenta encontrar por autocomplete ou tipo de input
                username_input = wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//input[@autocomplete='username' or @autocomplete='username webauthn' or @type='text']")
                ))
                login_pattern = "fallback"
                print("  → Detectado padrão de login: FALLBACK (xpath genérico)")
            except Exception:
                print("  ✗ Campo de usuário não encontrado na página")
                return False
        
        username_input.send_keys(usuario)
        
        # Preencher senha baseado no padrão detectado
        if login_pattern == "original":
            password_input = driver.find_element(By.NAME, "password")
        elif login_pattern == "novo_ab":
            password_input = driver.find_element(By.NAME, "pass")
        else:
            # Fallback: busca por tipo password
            try:
                password_input = driver.find_element(By.XPATH, "//input[@type='password']")
            except:
                password_input = driver.find_element(By.NAME, "password")
        
        password_input.send_keys(senha)
        
        # Clicar em login - tenta múltiplas formas
        login_button = None
        
        # Método 1: botão submit (original)
        try:
            login_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        except:
            pass
        
        # Método 2: busca pelo texto "Entrar" ou "Log in" (novo padrão A/B)
        if login_button is None:
            try:
                login_button = driver.find_element(By.XPATH, "//div[contains(@role, 'none')]//span[contains(text(), 'Entrar') or contains(text(), 'Log in')]/ancestor::div[@role='none'][1]")
            except:
                pass
        
        # Método 3: busca por span com texto de login
        if login_button is None:
            try:
                login_button = driver.find_element(By.XPATH, "//span[contains(text(), 'Entrar') or contains(text(), 'Log in')]")
            except:
                pass
        
        if login_button:
            login_button.click()
        else:
            print("  ✗ Não foi possível encontrar o botão de login")
            return False
            
        time.sleep(60) #aumentado para poder pegar o token no email
        
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
        erro_msg = str(e).split('\n')[0][:80]  # Primeira linha, max 80 chars
        print(f"  ✗ Falha no login: {erro_msg}")
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
                                # Salvar apenas o JSON na pasta especificada
                                filename_json = os.path.join(output_folder, f"{username}_stories.json")
                                with open(filename_json, 'w', encoding='utf-8') as f:
                                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                                
                                print(f"  ✓ {username}")
                                time.sleep(delay)
                                return True
                            else:
                                print(f"  ✗ {username} - Dados dos stories não encontrados")
                                return False
                            
                        except Exception:
                            print(f"  ✗ {username} - Erro ao processar resposta")
                            return False
            
            except:
                continue
        
        print(f"  ✗ {username} - Stories não disponíveis ou perfil privado")
        return False
        
    except Exception:
        print(f"  ✗ {username} - Erro ao acessar página")
        return False


def capturar_multiplas_paginas(lista_usuarios, usuario_login, senha_login, delay=3, max_tentativas_login=3, output_folder="."):
    """Captura stories de múltiplos usuários"""
    
    # Configurar Chrome
    chrome_options = Options()
    # chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    driver = None
    
    try:
        print(f"Iniciando... [{len(lista_usuarios)} páginas]\n")
        
        # Tentar login com retry
        tentativa_login = 0
        login_sucesso = False
        
        while tentativa_login < max_tentativas_login and not login_sucesso:
            tentativa_login += 1
            
            if driver:
                driver.quit()
                time.sleep(5)
            
            # Usar webdriver-manager para baixar automaticamente o ChromeDriver correto
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            print(f"Login... (tentativa {tentativa_login}/{max_tentativas_login})")
            
            if fazer_login_instagram(driver, usuario_login, senha_login):
                print("✓ Login realizado\n")
                login_sucesso = True
            else:
                if tentativa_login < max_tentativas_login:
                    print(f"  Aguardando 10s para nova tentativa...\n")
                    time.sleep(10)

        
        if not login_sucesso:
            print("✗ Falha após todas as tentativas de login")
            return
        
        # Processar cada usuário
        print("Capturando:")
        sucesso = 0
        
        for username in lista_usuarios:
            if capturar_stories_usuario(driver, username, delay, output_folder):
                sucesso += 1
        
        # Resumo
        print(f"\nConcluído: {sucesso}/{len(lista_usuarios)}")
        
    except KeyboardInterrupt:
        print("\n\n⚠ Execução interrompida pelo usuário")
        
    except Exception as e:
        erro_tipo = type(e).__name__
        erro_msg = str(e).split('\n')[0][:100] if str(e) else "Erro desconhecido"
        print(f"\n✗ Erro inesperado ({erro_tipo}): {erro_msg}")
        
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    # Credenciais de login
    usuario_login = "testdevjg"
    senha_login = "Evalleiford10"
    
    # Lista de páginas para capturar
    lista_usuarios = [
        "sensacional",
        "despertei"
        # Adicione mais usuários aqui
        # "usuario2",
        # "usuario3",
    ]
    
    # Delay entre consultas (em segundos)
    delay = 5
    
    capturar_multiplas_paginas(lista_usuarios, usuario_login, senha_login, delay)
