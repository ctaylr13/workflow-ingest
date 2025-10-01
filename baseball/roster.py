import requests

BASE_API_URL = 'https://statsapi.mlb.com/api/v1/divisions?sportId=1&divisionId=200'

print('script started')
response = requests.get(BASE_API_URL)
response.raise_for_status()
print('response', response)
data_json = response.json()
print('data json', data_json)
