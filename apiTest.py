import requests
import pandas as pd
import io

response = requests.get("http://127.0.0.1:8000/rsRating/2026-03-23")
df = pd.read_pickle(io.BytesIO(response.content))
print(df)