"""
Local test script — simulates what API Gateway sends to Lambda.
Run this from your terminal to test without deploying to AWS.
"""
import json
import sys
sys.path.insert(0, 'src/query_handler')  # Add Lambda folder to Python path
 
from app import lambda_handler
 
def test(question: str):
    """Simulate an API Gateway call to the Lambda handler."""
    # This is exactly the structure API Gateway sends
    fake_event = {
        'body': json.dumps({'question': question})
    }
    print(f'\n{'='*60}')
    print(f'QUESTION: {question}')
    print('='*60)
 
    result = lambda_handler(fake_event, {})
 
    print(f'Status: {result["statusCode"]}')
    body = json.loads(result['body'])
 
    if result['statusCode'] == 200:
        print(f'Generated SQL:\n{body["sql"]}\n')
        print(f'Results ({body["count"]} rows):')
        for row in body['results']:
            print(f'  {row}')
    else:
        print(f'ERROR: {body}')
 
if __name__ == '__main__':
    # Test 1: Top products
    test('What are the top 3 products by total revenue?')
 
    # Test 2: Regional breakdown
    #test('Show me total revenue by region')
 
    # Test 3: Category filter
    #test('How many Electronics orders were placed?')
 
    # Test 4: Specific date range
    #test('What were total sales in February 2024?')

    # Edge case 1: Vague question — does it make a reasonable choice?
    #test('Show me sales')
 
    # Edge case 2: Multi-condition — can it handle AND?
    #test('What Electronics products sold more than 3 units?')
    
    # Edge case 3: Calculation — does it use the right column?
    #test('What is the average order value per customer segment?')
    
    # Edge case 4: Completely off-topic — what happens?
    #test('What is the capital of France?')

     


