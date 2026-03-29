from dataGet import getAllHistoryAdjustedPrices
from RSRating import calculateRsRating
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from apscheduler.schedulers.background import BackgroundScheduler
import io
import threading
import pandas as pd
from datetime import datetime

app = FastAPI() # 確保這裡沒有 docs_url=None
scheduler = BackgroundScheduler()

cachedData = {
    "df": pd.DataFrame(),
    "lastUpdated": None
}
dataLock = threading.Lock()

def reloadDataAndCalculateRS():
    try:
        print(f"[{datetime.now()}] 開始抓取資料...")
        if getAllHistoryAdjustedPrices() or cachedData["df"].empty:
            print("開始計算 RS Rating...")
            df = calculateRsRating()
            
            # 確保 date 欄位是 string 或是 datetime 方便後續比較
            if 'date' in df.columns:
                df['date'] = df['date'].astype(str)
            
            with dataLock:
                cachedData["df"] = df
                cachedData["lastUpdated"] = pd.Timestamp.now()
            print("完成更新快取資料")
    except Exception as e:
        print(f"排程任務出錯: {e}")
# 設定排程
scheduler.add_job(reloadDataAndCalculateRS, 'cron',hour=15, minute=0,id="dailyRsRatingUpdate")
scheduler.start()
reloadDataAndCalculateRS() # 啟動時先執行一次，確保資料已經準備好
job = scheduler.get_job("dailyRsRatingUpdate")
if job:
    job.modify(next_run_time=datetime.now())

@app.get("/")
async def root():
    return {
        "status": "ok", 
        "lastUpdated": str(cachedData["lastUpdated"]) if cachedData["lastUpdated"] else "Never"
    }

@app.get("/rsRating")
async def getRsRating():
    # 1. 僅從快取中取出原始資料，縮短 Lock 時間
    with dataLock:
        fullDf = cachedData["df"].copy()
    
    if fullDf.empty:
        raise HTTPException(status_code=404, detail="Data is not ready yet.")

    try:
        # 2. 篩選資料(五年內)，這部分不需要 Lock，因為我們已經有 fullDf 的副本了
        filteredDf = fullDf[fullDf['date'] >= (pd.Timestamp.now() - pd.DateOffset(years=5)).strftime('%Y-%m-%d')]
        filteredDf = filteredDf[['stock_id', 'date', 'rsRating']]
        filteredDf['rsRating'] = filteredDf['rsRating'].astype('int8')

        # 3. 序列化
        buffer = io.BytesIO()
        filteredDf.to_pickle(buffer)
        return Response(
            content=buffer.getvalue(), 
            media_type="application/octet-stream"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filtering error: {str(e)}")

@app.post("/runTaskNow")
def runTaskNow():
    job = scheduler.get_job("dailyRsRatingUpdate")
    if job:
        job.modify(next_run_time=datetime.now())
        return {"status": "Manual trigger sent", "time": str(datetime.now())}
    else:
        # 如果找不到 job，嘗試手動執行函式
        return {"error": "Job ID not found"}, 404