from sentry_client import SentryClient, DEFAULT_REPORT_PATH

def test_sentry_connection():
    """
    Test the Sentry API connection and generate a multi-sheet report.
    Verifies the connection to Sentry and generates a report with different
    categories of issues based on their priority and type.

    Returns:
        bool: True if connection and report generation were successful, False otherwise.
    """
    try:
        client = SentryClient()
        
        # Gera o relatório com múltiplas abas
        print("\nGerando relatório com múltiplas abas...")
        dataframes = client.generate_multi_sheet_report()
        
        # Conta o número de issues em cada aba
        for sheet_name, df in dataframes.items():
            issues_count = len(df) if not df.empty else 0
            print(f"\n{sheet_name}: {issues_count} issues encontradas")
        
        # Salva o relatório final
        client.save_multi_sheet_report(dataframes)
        print(f"\nRelatório multi-abas gerado com sucesso em {DEFAULT_REPORT_PATH}!")
        
        return True
    except Exception as e:
        print(f"Erro ao conectar com o Sentry: {str(e)}")
        return False

if __name__ == "__main__":
    test_sentry_connection()