from fastapi import FastAPI
from fastapi.responses import Response
import io
import pandas as pd

app = FastAPI() # 確保這裡沒有 docs_url=None

@app.get("/")
async def root():
    return {"status": "ok", "message": "FastAPI is running"}

@app.get("/rsRating/{startDate}")
async def getRsRating(startDate: str):
    # 建立範例 DataFrame
    df = pd.read_pickle("cache/2026-03-24_RS.pkl")
    #把df處理成startDate~最後一天的資料
    df = df[df['date'] >= startDate]
    # 使用 BytesIO 在記憶體中處理二進位資料
    buffer = io.BytesIO()
    df.to_pickle(buffer)
    pickleData = buffer.getvalue()
    
    return Response(
        content=pickleData, 
        media_type="application/octet-stream"
    )