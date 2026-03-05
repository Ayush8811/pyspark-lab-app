import urllib.request
import json

data = json.dumps({
    "code": "df_emp = spark.sql('SELECT * FROM employees')\ndf_emp.show()",
    "datasets": {"employees": [{"id": 1, "name": "Alice"}]}
}).encode('utf-8')

req = urllib.request.Request('http://localhost:8000/api/problem/execute', data=data, headers={'Content-Type': 'application/json'})

try:
    with urllib.request.urlopen(req) as response:
        with open('response_output.json', 'w') as f:
            f.write(response.read().decode('utf-8'))
except Exception as e:
    with open('response_output.json', 'w') as f:
        f.write(str(e))
