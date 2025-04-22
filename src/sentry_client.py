import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import time
from dotenv import load_dotenv
from config import (
    SENTRY_AUTH_TOKEN, SENTRY_ORG, SENTRY_PROJECT, SENTRY_URL, 
    DEFAULT_REPORT_PATH, TOGETHER_API_KEY
)
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
from pathlib import Path

load_dotenv()

class SentryClient:
    NOT_AVAILABLE = "Não disponível"
    CACHE_FILE = "issue_summaries_cache.json"
    CACHE_EXPIRY = 3600  # 1 hora em segundos
    MAX_RETRIES = 3
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {SENTRY_AUTH_TOKEN}',
            'Content-Type': 'application/json'
        })
        self.base_url = SENTRY_URL
        self.last_summary_request = datetime.now()
        self.summary_requests_count = 0
        self.summary_rate_limit = 10
        self.summary_rate_window = 60
        self._priority_cache = {}
        self._rate_limit_lock = threading.Lock()
        self._summary_cache = self._load_summary_cache()
        self._translation_rate_limit = 50  # Requisições por minuto para Together AI
        self._last_translation = datetime.now()
        self._translation_count = 0
        
        # Pré-carrega as prioridades
        self._get_all_issues_with_priorities()
        
    def _load_summary_cache(self):
        """Load summaries cache from file"""
        try:
            cache_path = Path(self.CACHE_FILE)
            if cache_path.exists():
                with open(cache_path, 'r') as f:
                    cache_data = json.load(f)
                # Limpa entradas expiradas
                now = time.time()
                cache_data = {
                    k: v for k, v in cache_data.items()
                    if now - v.get('timestamp', 0) < self.CACHE_EXPIRY
                }
                return cache_data
        except Exception as e:
            print(f"Erro ao carregar cache: {e}")
        return {}

    def _save_summary_cache(self):
        """Save summaries cache to file"""
        try:
            with open(self.CACHE_FILE, 'w') as f:
                json.dump(self._summary_cache, f)
        except Exception as e:
            print(f"Erro ao salvar cache: {e}")

    def _make_request(self, method, endpoint, retry_count=0, **kwargs):
        """
        Centralized method for making HTTP requests with exponential retry.
        
        Args:
            method (str): HTTP method to use
            endpoint (str): API endpoint to call
            retry_count (int): Current retry attempt number
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response: The HTTP response from the API
            
        Raises:
            RequestException: If the request fails after all retries
        """
        try:
            url = f'{self.base_url}{endpoint}'
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = (2 ** retry_count) * 1
                print(f"Erro na requisição: {e}. Tentando novamente em {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(method, endpoint, retry_count + 1, **kwargs)
            raise

    def get_organization_info(self):
        """Get information about the current organization"""
        endpoint = f'/organizations/{SENTRY_ORG}/'
        response = self._make_request('GET', endpoint)
        return response.json()

    def get_project_info(self):
        """Get information about the current project"""
        endpoint = f'/projects/{SENTRY_ORG}/{SENTRY_PROJECT}/'
        response = self._make_request('GET', endpoint)
        return response.json()

    def get_issues(self, query_params=None):
        """
        Get issues from the project with custom query parameters.

        Args:
            query_params (dict, optional): Custom parameters for the query. Defaults to None.

        Returns:
            list: List of issues matching the query parameters.
        """
        endpoint = f'/organizations/{SENTRY_ORG}/issues/'
        
        # Parâmetros base para todas as consultas
        params = {
            'project': int(SENTRY_PROJECT),
            'statsPeriod': '24h',
            'limit': 100,
            'sort': 'freq',
            'environment': 'production',  # Sempre filtra por produção
            'expand': ['owners', 'inbox'],  # Expande informações importantes
            'shortIdLookup': '1'
        }
        
        # Atualiza com parâmetros personalizados se fornecidos
        if query_params:
            params.update(query_params)
        
        print("\nFazendo requisição para:", endpoint)
        print("Com parâmetros:", params)
        
        response = self._make_request('GET', endpoint, params=params)
        issues = response.json()
        
        print("\nDetalhes da resposta")
        print("Total de issues retornadas:", len(issues))
        
        return issues

    def _check_rate_limit(self):
        """
        Thread-safe rate limit check.
        Handles the rate limiting logic with proper thread synchronization.
        """
        with self._rate_limit_lock:
            current_time = datetime.now()
            time_diff = (current_time - self.last_summary_request).total_seconds()
            
            # Se passou a janela de tempo, reseta o contador
            if time_diff >= self.summary_rate_window:
                self.summary_requests_count = 0
                self.last_summary_request = current_time
            
            # Se atingiu o limite, espera até poder fazer nova requisição
            if self.summary_requests_count >= self.summary_rate_limit:
                wait_time = self.summary_rate_window - time_diff
                if wait_time > 0:
                    print(f"\nAguardando {wait_time:.1f} segundos para respeitar o rate limit...")
                    time.sleep(wait_time)
                    self.summary_requests_count = 0
                    self.last_summary_request = datetime.now()

    def _check_translation_rate_limit(self):
        """
        Thread-safe rate limit check for translations.
        Implements a rolling window rate limit for Together AI API.
        """
        with self._rate_limit_lock:
            current_time = datetime.now()
            time_diff = (current_time - self._last_translation).total_seconds()
            
            # Se passou 1 minuto, reseta o contador
            if time_diff >= 60:
                self._translation_count = 0
                self._last_translation = current_time
            
            # Se atingiu o limite, espera até poder fazer nova requisição
            if self._translation_count >= self._translation_rate_limit:
                wait_time = 60 - time_diff
                if wait_time > 0:
                    print(f"\nAguardando {wait_time:.1f} segundos para respeitar o rate limit da API de tradução...")
                    time.sleep(wait_time)
                    self._translation_count = 0
                    self._last_translation = datetime.now()

    def _translate_with_ai(self, text):
        """
        Translate text from English to Portuguese using Together AI.
        Preserves technical terms and provides natural Portuguese translations.
        
        Args:
            text (str): Text to translate
            
        Returns:
            str: Translated text
        """
        if not text or text == self.NOT_AVAILABLE:
            return self.NOT_AVAILABLE
            
        try:
            headers = {
                "Authorization": f"Bearer {TOGETHER_API_KEY}",
                "Content-Type": "application/json"
            }
            
            data = {
                "model": "mistralai/Mixtral-8x7B-Instruct-v0.1",
                "prompt": f"""### System: You are a specialized technical translator for software error messages and stack traces.
Your task is to translate from English to Brazilian Portuguese while following these strict rules:

1. NEVER translate these technical elements:
   - Error types and their hierarchy (e.g. Exception, TypeError, ValueError, KeyError)
   - Process/worker names (e.g. ForkPoolWorker-1, CeleryWorker)
   - System signals (e.g. SIGKILL, SIGTERM, SIGINT)
   - Stack trace components and line numbers
   - Variable names, parameters, and attributes
   - Database error codes and states
   - HTTP status codes and methods
   - API endpoint paths and parameters
   - Package and module names
   - Class and function names
   - File paths and extensions
   - Environment names (e.g. production, staging)
   - JSON keys and data structure terms
   - Git commands and states
   - Configuration keys
   - Protocol names and versions
   - Queue names and routing keys

2. Structure preservation rules:
   - Keep all numbers, dates, and metrics exactly as they appear
   - Preserve all punctuation and formatting
   - Maintain any indentation in stack traces
   - Keep all brackets, parentheses and their contents unchanged
   - Preserve all quotes and their content when they contain technical information

3. Translation guidelines:
   - Translate surrounding context into clear, professional Brazilian Portuguese
   - Use standard technical terminology common in Brazilian software development
   - Keep error descriptions concise and technically accurate
   - Maintain the original message's severity level and technical meaning
   - Use appropriate Portuguese technical jargon where it exists

### User: Translate this technical message to Portuguese while keeping technical terms unchanged:
{text}

### Assistant:""",
                "temperature": 0.3,
                "top_p": 0.7,
                "top_k": 50,
                "max_tokens": 200,
                "repetition_penalty": 1.1,
                "stop": ["###", "User:", "Assistant:", "System:"]
            }
            
            # Aumenta o timeout para 30 segundos
            response = requests.post(
                "https://api.together.xyz/inference",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if "choices" in result:
                    return result["choices"][0]["text"].strip()
                return text

            if response.status_code == 429:
                # Aumenta o tempo de espera para 10 segundos
                print("\nRate limit atingido. Aguardando 10s...")
                time.sleep(10)
                return self._translate_with_ai(text)
            
            return text

        except Exception as e:
            print(f"Erro ao traduzir texto: {str(e)}")
            return text

    def get_issue_summary(self, issue_id):
        """
        Get the AI summary analysis for a specific issue using POST request.
        
        Args:
            issue_id (str): The ID of the issue
            
        Returns:
            tuple: (whats_wrong, possible_cause) strings from the summary analysis, translated to Portuguese
        """
        # Verifica o cache primeiro
        cache_entry = self._summary_cache.get(issue_id)
        if (cache_entry and 
            (time.time() - cache_entry['timestamp'] < self.CACHE_EXPIRY)):
            return cache_entry['whats_wrong'], cache_entry['possivel_causa']

        self._check_rate_limit()
        
        endpoint = f'/organizations/{SENTRY_ORG}/issues/{issue_id}/summarize/'
        
        try:
            response = self._make_request('POST', endpoint)
            self.summary_requests_count += 1
            
            if response.status_code == 429:
                print("\nRate limit atingido. Aguardando.")
                time.sleep(self.summary_rate_window)
                self.summary_requests_count = 0
                self.last_summary_request = datetime.now()
                # Tenta novamente após esperar
                return self.get_issue_summary(issue_id)
            
            if response.status_code == 200:
                summary = response.json()
                whats_wrong = summary.get('whatsWrong', '').replace('*', '') or self.NOT_AVAILABLE
                possible_cause = summary.get('possibleCause', '').replace('*', '') or self.NOT_AVAILABLE
                
                # Traduz os textos usando IA
                if whats_wrong != self.NOT_AVAILABLE:
                    whats_wrong = self._translate_with_ai(whats_wrong)
                if possible_cause != self.NOT_AVAILABLE:
                    possible_cause = self._translate_with_ai(possible_cause)
                
                # Atualiza o cache com os textos traduzidos
                self._summary_cache[issue_id] = {
                    'whats_wrong': whats_wrong,
                    'possivel_causa': possible_cause,
                    'timestamp': time.time()
                }
                self._save_summary_cache()
                
                return whats_wrong, possible_cause
                
        except Exception as e:
            print(f"Erro ao obter sumário para issue {issue_id}: {str(e)}")
        
        return self.NOT_AVAILABLE, self.NOT_AVAILABLE

    def _process_summary_batch(self, batch_ids):
        """
        Process a batch of summaries in parallel using thread pool.
        
        Args:
            batch_ids (list): List of issue IDs to process
            
        Returns:
            list: List of summary tuples (whats_wrong, possible_cause)
        """
        with ThreadPoolExecutor(max_workers=min(len(batch_ids), 5)) as executor:
            future_to_id = {
                executor.submit(self.get_issue_summary, id): id 
                for id in batch_ids
            }
            
            results = []
            for future in as_completed(future_to_id):
                results.append(future.result())
            
            return results

    def create_issues_dataframe(self, issues):
        """
        Convert a list of issues into a DataFrame.
        Process issues in batches to respect rate limiting.
        Uses vectorized operations and parallel processing for better performance.

        Args:
            issues (list): List of issues from the Sentry API.

        Returns:
            pd.DataFrame: DataFrame containing the formatted issues data.
        """
        if not issues:
            return pd.DataFrame()
        
        data = []
        
        # Processamos os sumários em lotes menores para melhor controle
        batch_size = 5  # Reduzido para melhor gerenciamento do rate limit
        for i in range(0, len(issues), batch_size):
            batch = issues[i:i + batch_size]
            print(f"\nProcessando sumários do lote {i+1} até {min(i+batch_size, len(issues))} de {len(issues)}")
            
            # Processa cada issue do lote
            for issue in batch:
                issue_id = issue.get('id')
                
                # Obtém e traduz o sumário
                what_happened, possible_cause = self.get_issue_summary(issue_id)
                
                # Força a tradução se não estiver em português
                if what_happened != self.NOT_AVAILABLE and not self._is_portuguese(what_happened):
                    what_happened = self._translate_with_ai(what_happened)
                if possible_cause != self.NOT_AVAILABLE and not self._is_portuguese(possible_cause):
                    possible_cause = self._translate_with_ai(possible_cause)
                
                # Cria dicionário com todos os dados da issue
                issue_data = {
                    'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'title': issue.get('title'),
                    'count': issue.get('count', 0),
                    'users_affected': issue.get('userCount', 0),
                    'environment': issue.get('environment', 'all'),
                    'status': issue.get('status', 'unknown'),
                    'level': issue.get('level', 'unknown'),
                    'first_seen': issue.get('firstSeen'),
                    'last_seen': issue.get('lastSeen'),
                    'short_id': issue.get('shortId', ''),
                    'culprit': issue.get('culprit', ''),
                    'permalink': issue.get('permalink'),
                    'o_que_aconteceu': what_happened,
                    'possivel_causa': possible_cause
                }
                data.append(issue_data)
            
            if i + batch_size < len(issues):
                print("\nPausa entre lotes para respeitar rate limits...")
                time.sleep(2)
        
        # Criamos o DataFrame com todos os dados
        df = pd.DataFrame(data)
        
        # Ordena por frequência usando vetorização
        if not df.empty:
            df = df.sort_values(['count', 'users_affected'], ascending=[False, False])
        
        return df

    def _is_portuguese(self, text):
        """
        Verifica se o texto já está em português usando heurísticas simples.
        """
        if not text:
            return False
            
        # Palavras comuns em português que não existem em inglês
        portuguese_words = {'erro', 'falha', 'durante', 'com', 'foi', 'está', 'não', 'na', 'da', 'do', 'em', 'para'}
        words = set(text.lower().split())
        
        # Se encontrar pelo menos 2 palavras em português, considera que já está traduzido
        return len(words.intersection(portuguese_words)) >= 2

    def _filter_by_priority_level(self, df, priority_level):
        """
        Filter DataFrame by priority level using API values.
        priority_level can be: 'low', 'medium', 'high' or 'medium_high'
        
        Args:
            df (DataFrame): The DataFrame to filter
            priority_level (str): Priority level to filter by
            
        Returns:
            DataFrame: Filtered DataFrame containing only issues with matching priority
        """
        if df.empty:
            return df
            
        titles = df['title'].unique()
        priorities = {title: self._get_issue_priority(title) for title in titles}
        priority_series = df['title'].map(priorities)
        
        if priority_level == 'low':
            return df[priority_series == 'low']
        elif priority_level == 'medium':
            return df[priority_series == 'medium']
        elif priority_level == 'high':
            return df[priority_series == 'high']
        elif priority_level == 'medium_high':
            return df[priority_series.isin(['medium', 'high'])]
        return df

    def _get_issue_priority(self, issue_title):
        """
        Get priority directly from Sentry API
        
        Args:
            issue_title (str): The title of the issue
            
        Returns:
            str: Priority level ('low', 'medium', or 'high')
        """
        if issue_title in self._priority_cache:
            return self._priority_cache[issue_title]
        return 'low'  # default se não encontrado

    def save_multi_sheet_report(self, dataframes, output_path=None):
        """
        Save the report with multiple sheets to an Excel file.
        Creates all sheets even if they are empty.

        Args:
            dataframes (dict): Dictionary containing DataFrames for each sheet.
            output_path (str, optional): Path to save the Excel file. Defaults to DEFAULT_REPORT_PATH.

        Returns:
            bool: True if the report was saved successfully.
        """
        output_path = output_path or DEFAULT_REPORT_PATH
        
        # Remove arquivo temporário do Excel se existir
        temp_file = f'~${output_path}'
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError as e:
                print(f"Erro ao remover arquivo temporário: {e}")
        
        # Cria o arquivo Excel com múltiplas abas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Garante que todas as abas existam, mesmo que vazias
            for sheet_name, df in dataframes.items():
                if df.empty:
                    empty_df = pd.DataFrame(columns=[
                        'report_date', 'title', 'count', 'users_affected',
                        'environment', 'status', 'level', 'first_seen',
                        'last_seen', 'short_id', 'culprit', 'permalink',
                        'o_que_aconteceu', 'possivel_causa'
                    ])
                    empty_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"\nAba {sheet_name} criada vazia")
                else:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"\nAba {sheet_name} criada com {len(df)} registros")
        
        print(f"\nRelatório multi-abas salvo em: {output_path}")
        return True

    def debug_issue_fields(self):
        """
        Debug method to check available fields in issues.

        Returns:
            dict: Example issue with all its fields, None if no issues found.
        """
        issues = self.get_issues({'query': 'is:unresolved'})
        if issues:
            print("\nExample issue fields:")
            example_issue = issues[0]
            for key, value in example_issue.items():
                print(f"{key}: {value}")
            return example_issue
        return None

    def _get_all_issues_with_priorities(self):
        """
        Get all issues with their priorities in a single request to optimize API calls.
        Updates the internal priority cache with values from the API.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = self._make_request('GET', f'/organizations/{SENTRY_ORG}/issues/', params={
                'project': int(SENTRY_PROJECT),
                'statsPeriod': '24h',
                'limit': 100,
                'sort': 'freq'
            })
            issues = response.json()
            
            # Cache all priorities at once
            for issue in issues:
                title = issue.get('title')
                if title:
                    # Pega a prioridade diretamente do campo priority
                    priority = issue.get('priority', 'low').lower()
                    self._priority_cache[title] = priority
            
            return True
        except Exception as e:
            print(f"Erro ao obter issues com prioridades: {str(e)}")
            return False

    def generate_multi_sheet_report(self):
        """
        Generate a report with multiple sheets using optimized filtering.
        Follows exact criteria:
        - Erros Med/Alto: unresolved + category:error + (medium or high priority)
        - Alta Prioridade: unresolved + high priority (any category)
        - Baixa Prioridade: unresolved + low priority (any category)
        - Outros: unresolved + (medium or high priority) + not category:error
        """
        # Obtemos todas as issues de uma vez com os filtros base
        base_query = 'is:unresolved'
        
        # 1. Erros médio/alto (unresolved + category:error + medium/high priority)
        high_med_errors_params = {
            'query': f'{base_query} issue.priority:[high,medium] issue.category:error'
        }
        high_med_errors = self.get_issues(high_med_errors_params)
        high_med_errors_df = self.create_issues_dataframe(high_med_errors)
        
        # 2. Alta prioridade (unresolved + high priority)
        high_priority_params = {
            'query': f'{base_query} issue.priority:high'
        }
        high_priority_issues = self.get_issues(high_priority_params)
        high_priority_df = self.create_issues_dataframe(high_priority_issues)
        
        # 3. Baixa prioridade (unresolved + low priority)
        low_priority_params = {
            'query': f'{base_query} issue.priority:low'
        }
        low_priority_issues = self.get_issues(low_priority_params)
        low_priority_df = self.create_issues_dataframe(low_priority_issues)
        
        # 4. Outros (unresolved + medium/high priority + not error)
        others_params = {
            'query': f'{base_query} issue.priority:[high,medium] !issue.category:error'
        }
        other_issues = self.get_issues(others_params)
        others_df = self.create_issues_dataframe(other_issues)
        
        # Debug das contagens
        print("\nDetalhes dos filtros:")
        print(f"Total de erros médios/altos: {len(high_med_errors_df)}")
        print(f"Total alta prioridade: {len(high_priority_df)}")
        print(f"Total baixa prioridade: {len(low_priority_df)}")
        print(f"Total outros: {len(others_df)}")
        
        return {
            'Erros_Med_Alta': high_med_errors_df,
            'Alta_Prioridade': high_priority_df,
            'Baixa_Prioridade': low_priority_df,
            'Nao_Erros': others_df
        }

    @lru_cache(maxsize=128)
    def get_initial_priority(self, issue_title):
        """
        Get the priority for a specific issue.
        Now uses the direct priority value from Sentry API.

        Args:
            issue_title (str): The title of the issue to search for.

        Returns:
            str: The priority value ('low', 'medium', or 'high')
        """
        # Verifica primeiro no cache local
        if issue_title in self._priority_cache:
            return self._priority_cache[issue_title]
        
        # Se não está no cache, atualiza o cache com todas as issues
        if self._get_all_issues_with_priorities():
            return self._priority_cache.get(issue_title, 'low')
        
        return 'low'