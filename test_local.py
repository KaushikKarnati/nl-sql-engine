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
    test('What are the top 5 most viewed IGN videos?')
    test('How many videos did IGN publish in 2025?')
    test('What is the average engagement rate for Long videos?')
    test('Which month had the most videos published?')


     


