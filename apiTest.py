import requests
import pandas as pd
import io
#response = requests.post("http://127.0.0.1:8000/runTaskNow")
response = requests.get("http://127.0.0.1:8000/rsRating/2026-03-24")

df = pd.read_pickle(io.BytesIO(response.content))
print(df)