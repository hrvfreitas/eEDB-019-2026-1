"""
Great Expectations Suite - CFPB Complaints
8 validações conforme apresentação do projeto
"""

from great_expectations.core import ExpectationSuite, ExpectationConfiguration

def create_cfpb_suite():
    """
    Cria suite com as 8 expectativas documentadas no projeto
    """
    
    suite = ExpectationSuite(
        expectation_suite_name="cfpb_complaints_quality_suite",
        meta={
            "notes": "Suite de qualidade para dados CFPB - 8 validações críticas"
        }
    )
    
    # E1: complaint_id não pode ser nulo
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "complaint_id",
                "meta": {
                    "description": "E1 - Chave primária - sem ID o registro não é rastreável"
                }
            }
        )
    )
    
    # E2: complaint_id deve ser único
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={
                "column": "complaint_id",
                "meta": {
                    "description": "E2 - Garante que não há duplicatas na ingestão"
                }
            }
        )
    )
    
    # E3: product deve estar no conjunto de valores válidos
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "product",
                "value_set": [
                    "Credit card",
                    "Credit card or prepaid card",
                    "Mortgage",
                    "Debt collection",
                    "Credit reporting",
                    "Credit reporting, credit repair services, or other personal consumer reports",
                    "Student loan",
                    "Bank account or service",
                    "Checking or savings account",
                    "Consumer Loan",
                    "Money transfer, virtual currency, or money service",
                    "Payday loan",
                    "Payday loan, title loan, or personal loan",
                    "Prepaid card",
                    "Vehicle loan or lease",
                    "Other financial service"
                ],
                "mostly": 0.95,  # Aceita 5% de produtos novos/desconhecidos
                "meta": {
                    "description": "E3 - Produto fora do conjunto indica dado corrompido"
                }
            }
        )
    )
    
    # E4: date_received deve ter formato de data válido
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": "date_received",
                "type_": "DATE",
                "meta": {
                    "description": "E4 - Datas malformadas quebram o cálculo de response_days"
                }
            }
        )
    )
    
    # E5: state deve ter exatamente 2 caracteres
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_value_lengths_to_equal",
            kwargs={
                "column": "state",
                "value": 2,
                "mostly": 0.98,  # Permite alguns nulls ou valores especiais
                "meta": {
                    "description": "E5 - Sigla americana - outros valores indicam erro de entrada"
                }
            }
        )
    )
    
    # E6: timely_response apenas 'Yes' ou 'No'
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "timely_response",
                "value_set": ["Yes", "No"],
                "mostly": 0.99,
                "meta": {
                    "description": "E6 - Campo booleano textual - outros valores corrompem o KPI"
                }
            }
        )
    )
    
    # E7: submitted_via canal de envio predefinido
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "submitted_via",
                "value_set": [
                    "Web",
                    "Phone",
                    "Referral",
                    "Postal mail",
                    "Fax",
                    "Email"
                ],
                "mostly": 0.99,
                "meta": {
                    "description": "E7 - Garante Web, Telefone, Carta, Fax ou Referência"
                }
            }
        )
    )
    
    # E8: company não pode ser nulo
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "company",
                "meta": {
                    "description": "E8 - Sem empresa não é possível responder às perguntas de negócio"
                }
            }
        )
    )
    
    return suite

if __name__ == "__main__":
    suite = create_cfpb_suite()
    print(f"Suite criada: {suite.expectation_suite_name}")
    print(f"Total de expectativas: {len(suite.expectations)}")
    
    for i, exp in enumerate(suite.expectations, 1):
        desc = exp.kwargs.get('meta', {}).get('description', 'Sem descrição')
        print(f"  E{i}: {desc}")
