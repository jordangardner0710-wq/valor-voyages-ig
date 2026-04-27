import json
with open("google-sa.json") as f:
    data = json.load(f)
print("Service account email:")
print(f"  {data['client_email']}")
print(f"\nProject ID: {data['project_id']}")
