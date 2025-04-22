"""
Configuration module for Sentry API credentials and settings.
Loads environment variables from .env file for secure credential management.
"""

from dotenv import load_dotenv
import os

load_dotenv()

# Configurações de autenticação do Sentry
SENTRY_AUTH_TOKEN = os.getenv('SENTRY_AUTH_TOKEN')
SENTRY_ORG = os.getenv('SENTRY_ORG')
SENTRY_PROJECT = '1283350'  # ID numérico do projeto Django
SENTRY_URL = 'https://us.sentry.io/api/0'  # URL específica da região

# Together AI
TOGETHER_API_KEY = os.getenv('TOGETHER_API_KEY')

# Configuração do caminho do relatório
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_REPORT_PATH = os.path.join(BASE_DIR, 'sentry_report.xlsx')