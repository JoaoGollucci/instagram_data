"""
Instagram Network Capture - Versão Otimizada para Cloud Run
Usa Selenium com CDP (Chrome DevTools Protocol) para capturar dados de stories
Mantém a mesma lógica do arquivo original que funciona localmente
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time
import re
import os
import tempfile
from google.cloud import storage
from typing import Optional, Dict, List
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extrair_json_stories(html_content: str) -> Optional[Dict]:
    """
    Extrai o JSON com dados dos stories do HTML
    
    Args:
        html_content: Conteúdo HTML da página
        
    Returns:
        Dict com os dados extraídos ou None se não encontrado
    """
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
        logger.error(f"Erro ao extrair JSON: {e}", exc_info=True)
        return None


def fazer_login_instagram(driver, usuario: str, senha: str, bucket_name: Optional[str] = None) -> bool:
    """
    Faz login no Instagram usando Selenium
    
    Args:
        driver: WebDriver do Selenium
        usuario: Nome de usuário do Instagram
        senha: Senha do Instagram
        bucket_name: Nome do bucket GCS para salvar screenshots de debug
        
    Returns:
        True se login bem-sucedido, False caso contrário
    """
    try:
        logger.info("=" * 70)
        logger.info("🔐 INICIANDO PROCESSO DE LOGIN")
        logger.info("=" * 70)
        
        # Passo 1: Acessar página de login
        logger.info("🌐 PASSO 1: Acessando página de login...")
        driver.get("https://www.instagram.com/")
        time.sleep(3)
        
        # Screenshot passo 1
        logger.info("📸 Capturando screenshot - Passo 1...")
        screenshot_data = driver.get_screenshot_as_png()
        screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot_file.write(screenshot_data)
        screenshot_file.close()
        if bucket_name:
            upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_step1_initial.png")
        os.unlink(screenshot_file.name)
        
        logger.info(f"📍 URL atual: {driver.current_url}")
        logger.info(f"📄 Título da página: {driver.title}")
        
        # Passo 2: Aguardar e localizar campos
        logger.info("")
        logger.info("📝 PASSO 2: Aguardando campos de login...")
        wait = WebDriverWait(driver, 30)
        
        try:
            username_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
            logger.info("✅ Campo de username encontrado!")
        except Exception as e:
            logger.error(f"❌ Erro ao encontrar campo de username: {e}")
            
            # Screenshot erro
            screenshot_data = driver.get_screenshot_as_png()
            screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            screenshot_file.write(screenshot_data)
            screenshot_file.close()
            if bucket_name:
                upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_error_no_username_field.png")
            os.unlink(screenshot_file.name)
            
            # Salvar HTML para debug
            html_content = driver.page_source
            html_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.html', encoding='utf-8')
            html_file.write(html_content)
            html_file.close()
            if bucket_name:
                upload_to_gcs(bucket_name, html_file.name, "debug/login_error_page.html")
            os.unlink(html_file.name)
            
            return False
        
        # Passo 3: Preencher username
        logger.info("")
        logger.info(f"👤 PASSO 3: Preenchendo username: {usuario}")
        try:
            username_input.clear()
            time.sleep(0.5)
            username_input.send_keys(usuario)
            time.sleep(1)
            logger.info(f"✅ Username preenchido: '{username_input.get_attribute('value')}'")
        except Exception as e:
            logger.error(f"❌ Erro ao preencher username: {e}")
            return False
        
        # Screenshot passo 3
        logger.info("📸 Capturando screenshot - Passo 3...")
        screenshot_data = driver.get_screenshot_as_png()
        screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot_file.write(screenshot_data)
        screenshot_file.close()
        if bucket_name:
            upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_step3_username.png")
        os.unlink(screenshot_file.name)
        
        # Passo 4: Preencher senha
        logger.info("")
        logger.info("🔒 PASSO 4: Preenchendo senha...")
        try:
            password_input = driver.find_element(By.NAME, "password")
            logger.info("✅ Campo de senha encontrado!")
            password_input.clear()
            time.sleep(0.5)
            password_input.send_keys(senha)
            time.sleep(1)
            logger.info(f"✅ Senha preenchida (length: {len(senha)})")
        except Exception as e:
            logger.error(f"❌ Erro ao preencher senha: {e}")
            return False
        
        # Passo 5: Clicar no botão "Show" da senha
        logger.info("")
        logger.info("👁️ PASSO 5: Clicando no botão 'Show' para revelar senha...")
        try:
            show_password_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Show') or contains(text(), 'Mostrar') or @aria-label='Show password' or @aria-label='Mostrar senha']")
            logger.info("✅ Botão 'Show' encontrado!")
            show_password_button.click()
            time.sleep(1)
            logger.info("✅ Senha revelada!")
            
            # Ler o valor da senha do campo para confirmar
            senha_visivel = password_input.get_attribute('value')
            logger.info(f"🔓 Senha visível no campo: '{senha_visivel}'")
            logger.info(f"🔍 Senha fornecida era: '{senha}'")
            logger.info(f"✅ Senhas conferem: {senha_visivel == senha}")
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível clicar no botão Show: {e}")
            logger.info(f"🔍 Senha fornecida (não visível): length={len(senha)}")
        
        # Screenshot passo 5 (com senha visível se possível)
        logger.info("📸 Capturando screenshot - Passo 5 (senha visível)...")
        screenshot_data = driver.get_screenshot_as_png()
        screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot_file.write(screenshot_data)
        screenshot_file.close()
        if bucket_name:
            upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_step5_password_visible.png")
        os.unlink(screenshot_file.name)
        
        # Passo 6: Clicar no botão de login
        logger.info("")
        logger.info("🖱️ PASSO 6: Clicando no botão de login...")
        try:
            login_button = driver.find_element(By.XPATH, "//button[@type='submit']")
            logger.info("✅ Botão de login encontrado!")
            logger.info(f"📝 Texto do botão: '{login_button.text}'")
            login_button.click()
            logger.info("✅ Clique realizado!")
        except Exception as e:
            logger.error(f"❌ Erro ao clicar no botão de login: {e}")
            return False
        
        # Passo 7: Aguardar processamento
        logger.info("")
        logger.info("⏳ PASSO 7: Aguardando processamento do login (15s)...")
        time.sleep(15)
        
        logger.info(f"📍 URL após login: {driver.current_url}")
        logger.info(f"📄 Título após login: {driver.title}")
        
        # Screenshot passo 7
        logger.info("📸 Capturando screenshot - Passo 7...")
        screenshot_data = driver.get_screenshot_as_png()
        screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot_file.write(screenshot_data)
        screenshot_file.close()
        if bucket_name:
            upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_step7_after_submit.png")
        os.unlink(screenshot_file.name)
        
        # Verificar se há CAPTCHA
        logger.info("🤖 Verificando se há CAPTCHA...")
        try:
            # Procurar por elementos indicadores de CAPTCHA
            captcha_indicators = [
                "//iframe[contains(@src, 'recaptcha')]",
                "//*[contains(text(), 'não sou um robô')]",
                "//*[contains(text(), \"I'm not a robot\")]",
                "//*[contains(@class, 'recaptcha')]",
                "//div[@id='recaptcha']",
                "//*[contains(text(), 'verificação')]",
                "//*[contains(text(), 'verification')]",
                "//*[contains(text(), 'Unusual activity')]",
                "//*[contains(text(), 'Atividade incomum')]"
            ]
            
            captcha_found = False
            for xpath in captcha_indicators:
                try:
                    elements = driver.find_elements(By.XPATH, xpath)
                    if elements:
                        captcha_found = True
                        logger.warning(f"⚠️ CAPTCHA DETECTADO via xpath: {xpath}")
                        break
                except:
                    continue
            
            if captcha_found or "challenge" in driver.current_url:
                logger.error("❌ CAPTCHA DETECTADO!")
                logger.error("   O Instagram está pedindo verificação 'não sou um robô'")
                logger.error("   Isso geralmente ocorre devido a:")
                logger.error("   1. Login de localização/IP desconhecido")
                logger.error("   2. Comportamento automatizado detectado")
                logger.error("   3. Muitas tentativas de login")
                logger.error("")
                logger.error("   SOLUÇÕES POSSÍVEIS:")
                logger.error("   1. Use credenciais de uma conta que já fez login deste IP antes")
                logger.error("   2. Faça login manual uma vez no Instagram pelo navegador normal do servidor")
                logger.error("   3. Use um proxy/VPN confiável")
                logger.error("   4. Adicione delays maiores entre tentativas")
                logger.error("   5. Configure User-Agent e headers mais realistas")
                
                # Salvar HTML da página de CAPTCHA para análise
                html_content = driver.page_source
                html_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.html', encoding='utf-8')
                html_file.write(html_content)
                html_file.close()
                if bucket_name:
                    upload_to_gcs(bucket_name, html_file.name, "debug/login_captcha_page.html")
                    logger.error(f"   📄 HTML do CAPTCHA salvo para análise: gs://{bucket_name}/debug/login_captcha_page.html")
                os.unlink(html_file.name)
                
                return False
            else:
                logger.info("✅ Nenhum CAPTCHA detectado")
                
        except Exception as e:
            logger.warning(f"⚠️ Erro ao verificar CAPTCHA: {e}")
        
        # Verificar se ainda está na página de login
        if "/accounts/login/" in driver.current_url:
            logger.error("❌ ERRO: Ainda na página de login após submeter!")
            
            # Procurar por mensagens de erro
            try:
                error_elements = driver.find_elements(By.XPATH, "//*[contains(@class, 'error') or contains(@id, 'error')]")
                if error_elements:
                    logger.error(f"❌ Mensagens de erro encontradas:")
                    for elem in error_elements:
                        if elem.text:
                            logger.error(f"   - {elem.text}")
            except:
                pass
            
            return False
        
        # Passo 8: Validar sessão
        logger.info("")
        logger.info("🔍 PASSO 8: Validando sessão...")
        logger.info("   Acessando home do Instagram...")
        driver.get("https://www.instagram.com/")
        time.sleep(3)
        
        logger.info(f"📍 URL da home: {driver.current_url}")
        logger.info(f"📄 Título da home: {driver.title}")
        
        # Screenshot passo 8
        logger.info("📸 Capturando screenshot - Passo 8...")
        screenshot_data = driver.get_screenshot_as_png()
        screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot_file.write(screenshot_data)
        screenshot_file.close()
        if bucket_name:
            upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_step8_validation.png")
        os.unlink(screenshot_file.name)
        
        # Passo 9: Verificar cookies
        logger.info("")
        logger.info("🍪 PASSO 9: Verificando cookies...")
        cookies = driver.get_cookies()
        logger.info(f"   Total de cookies: {len(cookies)}")
        
        important_cookies = ['sessionid', 'csrftoken', 'ds_user_id']
        cookies_found = {}
        for cookie_name in important_cookies:
            cookie = next((c for c in cookies if c['name'] == cookie_name), None)
            if cookie:
                logger.info(f"   ✅ Cookie '{cookie_name}': {cookie['value'][:20]}...")
                cookies_found[cookie_name] = True
            else:
                logger.warning(f"   ⚠️ Cookie '{cookie_name}' NÃO encontrado!")
                cookies_found[cookie_name] = False
        
        # Verificar sucesso
        sucesso = "/accounts/login/" not in driver.current_url and cookies_found.get('sessionid', False)
        
        logger.info("")
        logger.info("=" * 70)
        if sucesso:
            logger.info("✅ LOGIN REALIZADO E VALIDADO COM SUCESSO!")
        else:
            logger.error("❌ LOGIN FALHOU NA VALIDAÇÃO!")
            logger.error(f"   - Não está na página de login: {'/accounts/login/' not in driver.current_url}")
            logger.error(f"   - Cookie sessionid presente: {cookies_found.get('sessionid', False)}")
        logger.info("=" * 70)
            
        return sucesso
            
    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO NO LOGIN: {e}", exc_info=True)
        
        # Screenshot de erro
        try:
            screenshot_data = driver.get_screenshot_as_png()
            screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            screenshot_file.write(screenshot_data)
            screenshot_file.close()
            if bucket_name:
                upload_to_gcs(bucket_name, screenshot_file.name, "debug/login_error_critical.png")
            os.unlink(screenshot_file.name)
        except:
            pass
        
        return False


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str) -> bool:
    """
    Faz upload de um arquivo para o Google Cloud Storage
    
    Args:
        bucket_name: Nome do bucket GCS
        source_file_path: Caminho do arquivo local
        destination_blob_name: Nome do arquivo no GCS
        
    Returns:
        True se upload bem-sucedido, False caso contrário
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(source_file_path)
        logger.info(f"✓ Upload para GCS: gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        logger.error(f"✗ Erro no upload para GCS: {e}", exc_info=True)
        return False


def capturar_stories_usuario(
    driver, 
    username: str, 
    delay: int = 5, 
    bucket_name: Optional[str] = None, 
    gcs_folder: str = "json"
) -> Optional[Dict]:
    """
    Captura dados de stories de um usuário específico usando CDP
    
    Args:
        driver: WebDriver do Selenium
        username: Nome do usuário do Instagram
        delay: Delay em segundos entre ações
        bucket_name: Nome do bucket GCS (opcional)
        gcs_folder: Pasta no GCS para salvar JSON
        
    Returns:
        Dict com dados capturados ou None se falhar
    """
    url = f"https://www.instagram.com/stories/{username}/"
    
    try:
        # Verificar se ainda está logado antes de acessar stories
        logger.info("🔍 Verificando se ainda está logado...")
        current_url = driver.current_url
        logger.info(f"📍 URL atual antes de acessar stories: {current_url}")
        
        # Se estiver na página de login, falhou
        if "/accounts/login/" in current_url:
            logger.error("❌ Sessão perdida - redirecionado para login!")
            return None
        
        # Verificar cookies de sessão
        cookies = driver.get_cookies()
        sessionid = next((c for c in cookies if c['name'] == 'sessionid'), None)
        if not sessionid:
            logger.error("❌ Cookie 'sessionid' não encontrado - sessão pode ter expirado!")
            return None
        
        logger.info(f"✅ Cookie de sessão presente")
        
        logger.info(f"📍 Acessando URL: {url}")
        driver.get(url)
        
        # Aguardar carregamento
        logger.info(f"⏳ Aguardando {delay}s para carregar stories...")
        time.sleep(delay)
        
        # Verificar se foi redirecionado para login
        if "/accounts/login/" in driver.current_url:
            logger.error(f"❌ @{username} - Redirecionado para login! Sessão perdida.")
            return None
        
        logger.info(f"📍 URL atual após carregamento: {driver.current_url}")
        
        # Capturar e salvar screenshot
        logger.info(f"📸 Capturando screenshot da página...")
        screenshot_data = driver.get_screenshot_as_png()
        screenshot_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot_file.write(screenshot_data)
        screenshot_file.close()
        
        if bucket_name:
            screenshot_path = f"debug/{username}_screenshot.png"
            upload_to_gcs(bucket_name, screenshot_file.name, screenshot_path)
            logger.info(f"📸 Screenshot salvo: gs://{bucket_name}/{screenshot_path}")
        
        os.unlink(screenshot_file.name)
        
        # Capturar e salvar HTML
        logger.info(f"📄 Capturando HTML da página...")
        html_content = driver.page_source
        html_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.html', encoding='utf-8')
        html_file.write(html_content)
        html_file.close()
        
        if bucket_name:
            html_path = f"debug/{username}_page.html"
            upload_to_gcs(bucket_name, html_file.name, html_path)
            logger.info(f"📄 HTML salvo: gs://{bucket_name}/{html_path}")
        
        os.unlink(html_file.name)
        
        # Aguardar mais tempo para garantir que requisições foram feitas
        logger.info(f"⏳ Aguardando mais {delay}s para capturar requisições...")
        time.sleep(delay)
        
        # Capturar requisições via CDP (Chrome DevTools Protocol)
        logger.info(f"🔍 Capturando logs de performance (CDP)...")
        logs = driver.get_log('performance')
        logger.info(f"📊 Total de logs capturados: {len(logs)}")
        
        # Salvar todos os logs para debug
        logs_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8')
        json.dump([json.loads(log['message']) for log in logs], logs_file, indent=2, ensure_ascii=False)
        logs_file.close()
        
        if bucket_name:
            logs_path = f"debug/{username}_network_logs.json"
            upload_to_gcs(bucket_name, logs_file.name, logs_path)
            logger.info(f"📊 Network logs salvos: gs://{bucket_name}/{logs_path}")
        
        os.unlink(logs_file.name)
        
        endpoint_alvo = f"{username}/?r="
        logger.info(f"🎯 Procurando por endpoint: {endpoint_alvo}")
        
        urls_encontradas = []
        
        for i, log in enumerate(logs):
            try:
                message = json.loads(log['message'])
                method = message.get('message', {}).get('method', '')
                
                if method == 'Network.responseReceived':
                    response = message['message']['params']['response']
                    request_url = response.get('url', '')
                    
                    # Log de todas as URLs para debug
                    if 'instagram.com' in request_url and 'stories' in request_url:
                        urls_encontradas.append(request_url)
                        logger.info(f"🔗 URL encontrada [{i}]: {request_url[:150]}...")
                    
                    if endpoint_alvo in request_url:
                        logger.info(f"✅ Endpoint alvo encontrado! URL: {request_url[:200]}...")
                        request_id = message['message']['params']['requestId']
                        logger.info(f"🆔 Request ID: {request_id}")
                        
                        # Obter corpo da resposta via CDP
                        try:
                            logger.info(f"📥 Obtendo corpo da resposta via CDP...")
                            response_body = driver.execute_cdp_cmd(
                                'Network.getResponseBody',
                                {'requestId': request_id}
                            )
                            
                            body = response_body.get('body', '')
                            logger.info(f"📦 Tamanho do corpo da resposta: {len(body)} bytes")
                            
                            if response_body.get('base64Encoded', False):
                                logger.info(f"🔓 Decodificando base64...")
                                import base64
                                body = base64.b64decode(body).decode('utf-8', errors='ignore')
                                logger.info(f"📦 Tamanho após decodificação: {len(body)} bytes")
                            
                            # Salvar resposta raw para debug
                            raw_response_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.html', encoding='utf-8')
                            raw_response_file.write(body)
                            raw_response_file.close()
                            
                            if bucket_name:
                                raw_path = f"debug/{username}_response_raw.html"
                                upload_to_gcs(bucket_name, raw_response_file.name, raw_path)
                                logger.info(f"📄 Response raw salvo: gs://{bucket_name}/{raw_path}")
                            
                            os.unlink(raw_response_file.name)
                            
                            # Extrair JSON do HTML
                            logger.info(f"🔍 Extraindo JSON do HTML...")
                            json_data = extrair_json_stories(body)
                            
                            if json_data:
                                logger.info(f"✅ JSON extraído com sucesso!")
                                
                                # Salvar em arquivo temporário
                                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
                                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                                    temp_file = f.name
                                
                                # Upload para GCS se configurado
                                if bucket_name:
                                    gcs_path = f"{gcs_folder}/{username}_stories.json"
                                    upload_to_gcs(bucket_name, temp_file, gcs_path)
                                
                                # Limpar arquivo temporário
                                os.unlink(temp_file)
                                
                                logger.info(f"✓ @{username} - Dados capturados com sucesso")
                                time.sleep(delay)
                                return json_data
                            else:
                                logger.warning(f"⚠️ JSON não encontrado no corpo da resposta")
                                logger.info(f"📝 Primeiros 500 caracteres: {body[:500]}")
                                return None
                            
                        except Exception as e:
                            logger.error(f"❌ Erro ao processar resposta: {e}", exc_info=True)
                            return None
            
            except Exception as e:
                logger.debug(f"Erro ao processar log {i}: {e}")
                continue
        
        logger.warning(f"⚠️ Endpoint não encontrado nos logs de rede")
        logger.info(f"📋 Total de URLs relacionadas a stories encontradas: {len(urls_encontradas)}")
        for idx, url in enumerate(urls_encontradas[:10], 1):  # Mostrar primeiras 10
            logger.info(f"  {idx}. {url[:200]}...")
        
        return None
        
    except Exception as e:
        logger.error(f"❌ Erro ao capturar stories de @{username}: {e}", exc_info=True)
        return None


def capturar_multiplas_paginas(
    lista_usuarios: List[str],
    usuario_login: str,
    senha_login: str,
    delay: int = 5,
    max_tentativas_login: int = 3,
    bucket_name: Optional[str] = None,
    gcs_folder: str = "json"
) -> List[Dict]:
    """
    Captura stories de múltiplos usuários usando Selenium com CDP
    
    Args:
        lista_usuarios: Lista de usernames para capturar
        usuario_login: Usuário do Instagram para login
        senha_login: Senha do Instagram
        delay: Delay entre requisições em segundos
        max_tentativas_login: Número máximo de tentativas de login
        bucket_name: Nome do bucket GCS (opcional)
        gcs_folder: Pasta no GCS para salvar JSONs
        
    Returns:
        Lista com resultados das capturas
    """
    resultados = []
    
    # Configurar Chrome Options
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    
    # User-Agent mais realista e atualizado
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
    
    # Argumentos para parecer mais com navegador real e evitar detecção
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Headers adicionais para parecer mais humano
    chrome_options.add_argument('--lang=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7')
    chrome_options.add_argument('--accept-language=pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7')
    
    # IMPORTANTE: Habilitar CDP para capturar requisições de rede
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    # Configurar caminho do ChromeDriver para Cloud Run
    chrome_options.binary_location = '/usr/bin/google-chrome'
    
    driver = None
    
    try:
        logger.info(f"Iniciando Selenium... [{len(lista_usuarios)} páginas]")
        logger.info("=" * 60)
        
        # Tentar login com retry
        login_sucesso = False
        
        for tentativa in range(1, max_tentativas_login + 1):
            if driver:
                driver.quit()
                time.sleep(5)
            
            # Usar ChromeDriver do sistema (instalado no Dockerfile)
            service = Service('/usr/bin/chromedriver')
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Executar script para ocultar indicadores de automação
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            })
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info(f"Tentativa de login {tentativa}/{max_tentativas_login}")
            
            if fazer_login_instagram(driver, usuario_login, senha_login, bucket_name):
                logger.info("✓ Login realizado com sucesso")
                login_sucesso = True
                break
            else:
                logger.warning(f"✗ Login falhou (tentativa {tentativa})")
                if tentativa < max_tentativas_login:
                    logger.info("Aguardando 10s antes de tentar novamente...")
                    time.sleep(10)
        
        if not login_sucesso:
            logger.error("✗ Falha após todas as tentativas de login")
            return resultados
        
        # Processar cada usuário
        logger.info("")
        logger.info("Iniciando captura de stories:")
        logger.info("-" * 60)
        
        sucesso = 0
        falhas = 0
        
        for i, username in enumerate(lista_usuarios, 1):
            logger.info(f"[{i}/{len(lista_usuarios)}] Processando @{username}")
            
            resultado = capturar_stories_usuario(
                driver, 
                username, 
                delay, 
                bucket_name, 
                gcs_folder
            )
            
            if resultado:
                sucesso += 1
                resultados.append({"username": username, "data": resultado, "status": "success"})
            else:
                falhas += 1
                resultados.append({"username": username, "data": None, "status": "failed"})
        
        # Resumo
        logger.info("-" * 60)
        logger.info(f"✓ Captura concluída")
        logger.info(f"  Sucesso: {sucesso}/{len(lista_usuarios)}")
        logger.info(f"  Falhas: {falhas}/{len(lista_usuarios)}")
        logger.info("=" * 60)
        
        return resultados
        
    except Exception as e:
        logger.error(f"✗ Erro crítico na captura: {e}", exc_info=True)
        return resultados
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("Driver fechado")
            except:
                pass
