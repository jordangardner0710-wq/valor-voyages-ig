import os
import base64
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Quick text-only test first
resp = client.chat.completions.create(
    model=model,
    messages=[{"role": "user", "content": "Reply with exactly: PIPELINE_OK"}],
    max_tokens=10,
)
text = resp.choices[0].message.content.strip()
print(f"[OK] Model: {model}")
print(f"[OK] Response: {text}")
print(f"[OK] Tokens used: {resp.usage.total_tokens} (in: {resp.usage.prompt_tokens}, out: {resp.usage.completion_tokens})")
print(f"[OK] Approx cost: ${resp.usage.total_tokens * 0.00000015:.6f}")
