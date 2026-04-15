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
import subprocess
import glob

COOKIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies_instagram.json')


def salvar_cookies(driver):
    """Salva cookies do navegador em arquivo JSON."""
    try:
        cookies = driver.get_cookies()
        with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False)
    except Exception:
        pass


def carregar_cookies(driver):
    """Carrega cookies salvos no navegador. Retorna True se conseguiu."""
    try:
        if not os.path.exists(COOKIES_FILE):
            return False
        with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
            cookies = json.load(f)
        driver.get("https://www.instagram.com/")
        time.sleep(2)
        for cookie in cookies:
            # Remover campos que podem causar erro
            cookie.pop('sameSite', None)
            cookie.pop('expiry', None)
            try:
                driver.add_cookie(cookie)
            except Exception:
                continue
        return True
    except Exception:
        return False


def verificar_sessao_ativa(driver):
    """Verifica se já existe uma sessão ativa no Instagram."""
    try:
        driver.get("https://www.instagram.com/")
        time.sleep(4)
        url = driver.current_url

        # Se redirecionou para login/accounts, não está logado
        if "login" in url or "accounts" in url:
            return False

        # Verificar se existem elementos que só aparecem quando logado
        # (ícones de navegação, barra de busca, perfil, etc.)
        indicadores_logado = [
            "//svg[@aria-label='Página inicial' or @aria-label='Home']",
            "//svg[@aria-label='Pesquisar' or @aria-label='Search']",
            "//a[contains(@href, '/direct/')]",
            "//span[contains(text(), 'Pesquisar') or contains(text(), 'Search')]",
        ]
        for xpath in indicadores_logado:
            try:
                driver.find_element(By.XPATH, xpath)
                return True
            except Exception:
                continue

        # Se existem campos de login na página, não está logado
        try:
            driver.find_element(By.NAME, "username")
            return False
        except Exception:
            pass
        try:
            driver.find_element(By.NAME, "email")
            return False
        except Exception:
            pass

        return False
    except Exception:
        return False


def _esta_logado(driver):
    """Verifica se a página atual é a interface logada do Instagram."""
    try:
        indicadores = [
            "//svg[@aria-label='Página inicial' or @aria-label='Home']",
            "//svg[@aria-label='Pesquisar' or @aria-label='Search']",
            "//a[contains(@href, '/direct/')]",
        ]
        for xpath in indicadores:
            try:
                driver.find_element(By.XPATH, xpath)
                return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def _esta_em_tela_intermediaria(driver):
    """Detecta se está em alguma tela intermediária (2FA, challenge, verificação, captcha)."""
    try:
        url = driver.current_url
        # URLs conhecidas de telas intermediárias
        palavras_intermediarias = ["challenge", "two_factor", "suspicious", "checkpoint", "verify", "confirm"]
        for palavra in palavras_intermediarias:
            if palavra in url:
                return True
        # Detectar campos de código/token na página
        try:
            driver.find_element(By.XPATH, "//input[@name='security_code' or @name='verificationCode' or @id='security_code' or @autocomplete='one-time-code']")
            return True
        except Exception:
            pass
        # Detectar textos comuns de verificação
        try:
            driver.find_element(By.XPATH, "//*[contains(text(), 'código de segurança') or contains(text(), 'security code') or contains(text(), 'código de confirmação') or contains(text(), 'confirmation code') or contains(text(), 'Confirme que é você') or contains(text(), 'Confirm') or contains(text(), 'enviamos') or contains(text(), 'sent')]")
            return True
        except Exception:
            pass
        return False
    except Exception:
        return False


def aguardar_login_ou_2fa(driver, timeout=300):
    """Aguarda login com detecção inteligente de 2FA/captcha.
    Retorna: 'sucesso', 'erro_credenciais', 'timeout' ou 'timeout_2fa'."""
    inicio = time.time()
    estado_anterior = None
    em_2fa = False

    while (time.time() - inicio) < timeout:
        try:
            url = driver.current_url

            # Confirmação positiva de login (presença de elementos da interface logada)
            if _esta_logado(driver):
                return "sucesso"

            # Detectar estado atual para mensagem
            estado = None
            if _esta_em_tela_intermediaria(driver):
                estado = "2fa"
                em_2fa = True
            elif "login" in url or "accounts" in url:
                # Verificar se há erro de credenciais
                try:
                    driver.find_element(By.XPATH, "//*[contains(text(), 'senha') or contains(text(), 'password') or contains(text(), 'incorret')]")
                    estado = "erro_credenciais"
                except Exception:
                    estado = "login"
            else:
                # Página desconhecida - pode ser tela de 2FA nova
                # Não assumir sucesso, tratar como aguardando
                estado = "2fa"
                em_2fa = True

            # Exibir mensagem apenas quando o estado muda
            if estado != estado_anterior:
                restante = int(timeout - (time.time() - inicio))
                if estado == "2fa":
                    print(f"  ⏳ 2FA/verificação detectada - resolva no navegador (timeout: {restante}s)")
                elif estado == "erro_credenciais":
                    print("  ✗ Possível erro de credenciais detectado")
                    return "erro_credenciais"
                elif estado == "login":
                    print(f"  ⏳ Aguardando login... (timeout: {restante}s)")
                estado_anterior = estado

        except Exception:
            pass

        time.sleep(3)

    if em_2fa:
        print("  ✗ Timeout aguardando resolução de 2FA")
        return "timeout_2fa"
    else:
        print("  ✗ Timeout aguardando login")
        return "timeout"

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
        
        # Aguardar login com detecção inteligente de 2FA/captcha
        login_timeout = int(os.getenv('LOGIN_TIMEOUT', '300'))
        resultado = aguardar_login_ou_2fa(driver, timeout=login_timeout)
        
        if resultado != "sucesso":
            return resultado
        
        # Fechar popups
        for _ in range(2):
            try:
                not_now_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Agora não') or contains(text(), 'Not Now')]")
                not_now_button.click()
                time.sleep(1)
            except:
                pass
        
        logado = "login" not in driver.current_url
        if logado:
            salvar_cookies(driver)
            return "sucesso"
        return "falha"
            
    except Exception as e:
        erro_msg = str(e).split('\n')[0][:80]
        print(f"  ✗ Falha no login: {erro_msg}")
        return "erro"

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
    
    # Perfil persistente do Chrome (mantém sessão entre execuções)
    chrome_profile = os.getenv('CHROME_PROFILE_DIR', '')
    if chrome_profile:
        profile_path = os.path.abspath(chrome_profile)
        os.makedirs(profile_path, exist_ok=True)
        # Verificar integridade do perfil (Preferences corrompido impede o Chrome de abrir)
        prefs_file = os.path.join(profile_path, 'Default', 'Preferences')
        if os.path.exists(prefs_file):
            try:
                with open(prefs_file, 'r', encoding='utf-8') as f:
                    json.load(f)
            except (json.JSONDecodeError, ValueError):
                print("  ⚠ Perfil corrompido detectado - recriando...")
                import shutil
                shutil.rmtree(profile_path, ignore_errors=True)
                os.makedirs(profile_path, exist_ok=True)
        # Remover lock files de execução anterior
        for lock in glob.glob(os.path.join(profile_path, 'Singleton*')):
            try:
                os.remove(lock)
            except Exception:
                pass
        chrome_options.add_argument(f'--user-data-dir={profile_path}')
        print(f"Perfil Chrome: {profile_path}")
    
    driver = None
    
    try:
        print(f"Iniciando... [{len(lista_usuarios)} páginas]\n")
        
        # Encerrar processos órfãos que podem travar o perfil
        if chrome_profile:
            try:
                subprocess.run(
                    ['taskkill', '/F', '/IM', 'chrome.exe', '/T'],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                subprocess.run(
                    ['taskkill', '/F', '/IM', 'chromedriver.exe', '/T'],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                time.sleep(3)
                # Limpar lock files restantes
                for lock in glob.glob(os.path.join(profile_path, 'Singleton*')):
                    try:
                        os.remove(lock)
                    except Exception:
                        pass
            except Exception:
                pass
        
        # Criar driver (com retry em caso de SessionNotCreatedException)
        driver_criado = False
        for tentativa_driver in range(3):
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                driver_criado = True
                break
            except Exception as e:
                erro = str(e).split('\n')[0][:100]
                if tentativa_driver < 2:
                    print(f"  ⚠ Erro ao iniciar navegador: {erro}")
                    print(f"  Tentando novamente ({tentativa_driver + 2}/3)...")
                    # Matar tudo e limpar antes de tentar novamente
                    for proc in ['chrome.exe', 'chromedriver.exe']:
                        subprocess.run(
                            ['taskkill', '/F', '/IM', proc, '/T'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                        )
                    time.sleep(3)
                    for lock in glob.glob(os.path.join(profile_path, 'Singleton*')) if chrome_profile else []:
                        try:
                            os.remove(lock)
                        except Exception:
                            pass
                else:
                    print(f"  ✗ Não foi possível iniciar o navegador: {erro}")
                    return
        
        if not driver_criado:
            return
        
        # Verificar se já existe sessão ativa (perfil persistente ou cookies)
        login_sucesso = False
        
        print("Verificando sessão...")
        if verificar_sessao_ativa(driver):
            print("✓ Sessão ativa encontrada - login não necessário\n")
            login_sucesso = True
        else:
            # Tentar restaurar cookies se não tem perfil persistente
            if not chrome_profile and carregar_cookies(driver):
                driver.refresh()
                time.sleep(3)
                if verificar_sessao_ativa(driver):
                    print("✓ Sessão restaurada via cookies\n")
                    login_sucesso = True
        
        # Se nenhuma sessão ativa, fazer login com retry
        if not login_sucesso:
            tentativa_login = 0
            
            while tentativa_login < max_tentativas_login and not login_sucesso:
                tentativa_login += 1
                
                if tentativa_login > 1:
                    driver.quit()
                    time.sleep(5)
                    service = Service(ChromeDriverManager().install())
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                
                print(f"Login... (tentativa {tentativa_login}/{max_tentativas_login})")
                
                resultado = fazer_login_instagram(driver, usuario_login, senha_login)
                
                if resultado == "sucesso":
                    print("✓ Login realizado\n")
                    login_sucesso = True
                elif resultado == "timeout_2fa":
                    # 2FA apareceu mas o tempo acabou - NÃO fechar navegador,
                    # dar mais tempo no mesmo browser
                    print("  → Dando mais tempo para resolver 2FA...")
                    login_timeout = int(os.getenv('LOGIN_TIMEOUT', '300'))
                    resultado2 = aguardar_login_ou_2fa(driver, timeout=login_timeout)
                    if resultado2 == "sucesso":
                        # Fechar popups após 2FA resolvido
                        for _ in range(2):
                            try:
                                not_now_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Agora não') or contains(text(), 'Not Now')]")
                                not_now_button.click()
                                time.sleep(1)
                            except:
                                pass
                        salvar_cookies(driver)
                        print("✓ Login realizado (após 2FA)\n")
                        login_sucesso = True
                    else:
                        print("  ✗ Não foi possível completar o 2FA")
                        break  # Não adianta retentar, precisa de intervenção
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
