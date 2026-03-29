from FinMindApi import FinMindApi
import os
import datetime


def getDataDate():
    """從快取資料夾中找出最新的快取日期"""
    if not os.path.exists("data"):
        return None
    files = os.listdir("data")
    dates = []
    for file in files:
        if len(file) == 30 and file.endswith(".pkl"):
            dateStr = file.split("_")[2].split(".")[0]
            try:
                date = datetime.datetime.strptime(dateStr, "%Y-%m-%d")
                dates.append(date)
            except ValueError:
                continue
    if dates:
        latestDate = max(dates)
        return latestDate.strftime("%Y-%m-%d")
    else:
        return None


def getAllHistoryAdjustedPrices():
    """取得所有台灣股票的歷史調整股價"""
    api = FinMindApi()
    startDate = "1994-10-01"
    latestTradingDate = api.getLatestTradingDate()
    latestCacheDate = getDataDate()
    print(f"最新的交易日期是: {latestTradingDate}")
    print(f"最新的快取日期是: {latestCacheDate}")

    if latestCacheDate != latestTradingDate:
        print("快取資料不是最新的，將從 API 取得最新資料")
        if os.path.exists("data"):
            for file in os.listdir("data"):
                os.remove(os.path.join("data", file))
        getAllTaiwanStockInfo = api.getAllTaiwanStockInfo(latestTradingDate)
        if getAllTaiwanStockInfo is None:
            print("無法取得台灣股票資訊")
            exit()
        df = api.getData("TAIEX", startDate, latestTradingDate)
        if df is not None:
            print("成功取得 TAIEX 的資料")
        else:
            print("無法取得 TAIEX 的資料")
        excludeCategories = ['Index', '受益證券', 'ETF', 'ETN', '存託憑證', '受益憑證']
        twseData = getAllTaiwanStockInfo[
            (getAllTaiwanStockInfo['type'] == 'twse') &
            (getAllTaiwanStockInfo['date'] == latestTradingDate) &
            (getAllTaiwanStockInfo['stock_id'].str.len() == 4) &
            (~getAllTaiwanStockInfo['industry_category'].isin(
                excludeCategories))
        ]
        for index, row in twseData.iterrows():
            stockId = row['stock_id']
            df = api.getData(stockId, startDate, latestTradingDate)
            if df is not None:
                print(f"成功取得 {stockId} 的資料")
            else:
                print(f"無法取得 {stockId} 的資料")
        files = os.listdir("data")
        print(f"完成，總共處理了 {len(files)} 檔股票資料")
        return True
    else:
        print("快取資料已經是最新的，直接使用快取資料")
        files = os.listdir("data")
        print(f"共處理了 {len(files)} 檔股票資料")
        return False


if __name__ == "__main__":
    getAllHistoryAdjustedPrices()
