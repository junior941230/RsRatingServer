import requests
import pandas as pd
import io,time
choice = input("選擇任務:\n1. 立即執行排程任務\n2. 取得 RS Rating 資料\n請輸入選項 (1 或 2): ")
start_time = time.time()
if choice == "1":
    response = requests.post("http://127.0.0.1:8000/runTaskNow")
elif choice == "2":
    response = requests.get("http://api.9tsai.xyz/rsRating")
    df = pd.read_pickle(io.BytesIO(response.content))
    print(df)
print(f"任務完成，耗時 {time.time() - start_time:.2f} 秒")
#journalctl -u fastapi.service -f