import requests
from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
from config import SENTRY_AUTH_TOKEN, SENTRY_ORG, SENTRY_PROJECT, SENTRY_URL, DEFAULT_REPORT_PATH

load_dotenv()

class SentryClient:
    def __init__(self):
        self.headers = {
            'Authorization': f'Bearer {SENTRY_AUTH_TOKEN}',
            'Content-Type': 'application/json'
        }
        self.base_url = SENTRY_URL

    def get_organization_info(self):
        """Get information about the current organization"""
        endpoint = f'{self.base_url}/organizations/{SENTRY_ORG}/'
        response = requests.get(endpoint, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_project_info(self):
        """Get information about the current project"""
        endpoint = f'{self.base_url}/projects/{SENTRY_ORG}/{SENTRY_PROJECT}/'
        response = requests.get(endpoint, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_issues(self, query_params=None):
        """
        Get issues from the project with custom query parameters.

        Args:
            query_params (dict, optional): Custom parameters for the query. Defaults to None.

        Returns:
            list: List of issues matching the query parameters.
        """
        endpoint = f'{self.base_url}/organizations/{SENTRY_ORG}/issues/'
        
        # Parâmetros base para todas as consultas
        params = {
            'project': int(SENTRY_PROJECT),
            'statsPeriod': '24h',
            'limit': 100,
            'sort': 'freq'
        }
        
        # Atualiza com parâmetros personalizados se fornecidos
        if query_params:
            params.update(query_params)
        
        print(f"\nFazendo requisição para: {endpoint}")
        print(f"Com parâmetros: {params}")
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        
        if response.status_code != 200:
            print(f"Resposta da API: {response.text}")
            response.raise_for_status()
        
        issues = response.json()
        print(f"\nDetalhes da resposta:")
        print(f"Total de issues retornadas: {len(issues)}")
        
        return issues

    def create_issues_dataframe(self, issues):
        """
        Convert a list of issues into a DataFrame.

        Args:
            issues (list): List of issues from the Sentry API.

        Returns:
            pd.DataFrame: DataFrame containing the formatted issues data.
        """
        if not issues:
            return pd.DataFrame()
        
        report_data = []
        for issue in issues:
            report_data.append({
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
                'permalink': issue.get('permalink')
            })
        
        df = pd.DataFrame(report_data)
        
        # Adiciona timestamp de quando o relatório foi gerado
        df.insert(0, 'report_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Ordena por frequência (mais frequentes primeiro)
        if not df.empty:
            df = df.sort_values(['count', 'users_affected'], ascending=[False, False])
        
        return df

    def get_initial_priority(self, issue_title):
        """
        Get the initial priority for a specific issue.

        Args:
            issue_title (str): The title of the issue to search for.

        Returns:
            int: The initial priority value, defaults to 0 if not found.
        """
        query_params = {
            'query': f'title:"{issue_title}"',
            'statsPeriod': '24h'
        }
        
        issues = self.get_issues(query_params)
        if issues and len(issues) > 0:
            metadata = issues[0].get('metadata', {})
            return int(metadata.get('initial_priority', 0))
        return 0

    def generate_multi_sheet_report(self):
        """
        Generate a report with multiple sheets:
        1. Medium/High priority errors that are unresolved
        2. All high priority unresolved issues (any type)
        3. All low priority unresolved issues (any type)
        4. Any issue that is not an error

        Returns:
            dict: Dictionary containing DataFrames for each report sheet.
        """
        # Aba 1: Erros não resolvidos com prioridade média e alta
        high_med_errors_params = {
            'query': 'is:unresolved level:error',
            'statsPeriod': '24h'
        }
        high_med_errors = self.get_issues(high_med_errors_params)
        high_med_errors_df = self.create_issues_dataframe(high_med_errors)
        
        # Filtra erros de média e alta prioridade
        if not high_med_errors_df.empty:
            high_med_errors_df = high_med_errors_df[
                high_med_errors_df.apply(lambda x: self.get_initial_priority(x['title']) >= 25, axis=1)
            ]
        
        # Aba 2: Todas as issues não resolvidas de alta prioridade
        high_priority_params = {
            'query': 'is:unresolved',
            'statsPeriod': '24h'
        }
        high_priority_issues = self.get_issues(high_priority_params)
        high_priority_df = self.create_issues_dataframe(high_priority_issues)
        
        # Filtra alta prioridade
        if not high_priority_df.empty:
            high_priority_df = high_priority_df[
                high_priority_df.apply(lambda x: self.get_initial_priority(x['title']) >= 50, axis=1)
            ]
        
        # Aba 3: Todas as issues não resolvidas de baixa prioridade
        low_priority_params = {
            'query': 'is:unresolved',
            'statsPeriod': '24h'
        }
        low_priority_issues = self.get_issues(low_priority_params)
        low_priority_df = self.create_issues_dataframe(low_priority_issues)
        
        # Filtra baixa prioridade
        if not low_priority_df.empty:
            low_priority_df = low_priority_df[
                low_priority_df.apply(lambda x: self.get_initial_priority(x['title']) < 50, axis=1)
            ]
        
        # Aba 4: Todas as issues que não são erros
        non_errors_params = {
            'query': '!level:error',
            'statsPeriod': '24h'
        }
        non_errors = self.get_issues(non_errors_params)
        non_errors_df = self.create_issues_dataframe(non_errors)
        
        return {
            'Erros_Med_Alta': high_med_errors_df,
            'Alta_Prioridade': high_priority_df,
            'Baixa_Prioridade': low_priority_df,
            'Nao_Erros': non_errors_df
        }

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
            except:
                pass
        
        # Cria o arquivo Excel com múltiplas abas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Garante que todas as abas existam, mesmo que vazias
            for sheet_name, df in dataframes.items():
                if df.empty:
                    # Cria aba vazia com cabeçalhos
                    empty_df = pd.DataFrame(columns=[
                        'report_date', 'title', 'count', 'users_affected',
                        'environment', 'status', 'level', 'first_seen',
                        'last_seen', 'short_id', 'culprit', 'permalink'
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