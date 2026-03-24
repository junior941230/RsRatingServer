from FinMind.data import DataLoader
import json
import os
import pandas as pd
import datetime


class FinMindApi:
    """處理 FinMind API 的初始化和資料取得，包含快取機制"""

    def __init__(self):
        # 嘗試從 token.json 載入 API 令牌
        try:
            with open('token.json', 'r') as f:
                tokenInfo = json.load(f)
                apiToken = tokenInfo.get('token', '')
        except FileNotFoundError:
            apiToken = ""
        if apiToken:
            self.api = DataLoader()
            self.api.login_by_token(api_token=apiToken)
        apiUsage, apiUsageLimit = self.apiUsageCheck()
        print(f"目前 API 使用量: {apiUsage}/{apiUsageLimit}")

    def apiUsageCheck(self):
        """檢查 API 使用量，若超過 90% 則發出警告"""
        if (self.api.api_usage >= self.api.api_usage_limit * 0.9):
            print("API 次數已達90%，請注意使用量！")
        return self.api.api_usage, self.api.api_usage_limit

    def getMarketValue(self):
        """取得所有台灣股票市值，每日快取一次"""
        if not os.path.exists("cache"):
            os.makedirs("cache")
        files = os.listdir("cache")
        # date = datetime.datetime.today().strftime("%Y-%m-%d")
        date = "2026-03-20"
        fileName = f"{date}_MarketValue.pkl"
        if fileName in files:
            print("當日以查詢")
            df = pd.read_pickle("cache/" + fileName)
            return df
        else:
            if self.api is None:
                return None
            df = self.api.taiwan_stock_market_value(
                start_date=date,
            )
            df.to_pickle("cache/" + fileName)
            return df

    def getData(self, stockId, startDate, endDate):
        """
        取得股票資料，優先從快取中查詢
        若無快取則從 API 取得並儲存到本地快取
        """
        if not os.path.exists("data"):
            os.makedirs("data")
        cachedData = self.findCacheData(stockId, startDate, endDate)
        if cachedData is not None:
            return cachedData
        if self.api is None:
            return None
        rawDf = self.api.taiwan_stock_daily_adj(
            stock_id=stockId,
            start_date=startDate,
            end_date=endDate
        )
        rawDf.to_pickle(f"data/{stockId}_{startDate}_{endDate}.pkl")
        return rawDf

    def findCacheData(self, stockId, startDate, endDate):
        """
        在快取資料夾中搜尋符合條件的資料
        快取資料的日期範圍必須涵蓋使用者要求的日期範圍
        """
        files = os.listdir("data")
        for file in files:
            endDateInFile = file.split("_")[-1].split(".")[0]
            startDateInFile = file.split("_")[1]
            if f"{stockId}" in file:
                # 檔案的日期範圍包含了使用者要求的日期範圍，才算找到快取資料
                if endDateInFile >= endDate and startDateInFile <= startDate:
                    cachedData = pd.read_pickle(f"data/{file}")
                    # print("找到快取資料，直接使用")
                    # 轉換 date 欄位為 datetime 格式，才能使用 between 方法過濾日期範圍
                    cachedData['date'] = pd.to_datetime(cachedData['date'])
                    filteredDf = cachedData[cachedData['date'].between(
                        startDate, endDate)]
                    return filteredDf
        # print("沒有找到快取資料，將從 API 取得")
        return None

    def getAllTaiwanStockInfo(self):
        """取得所有台灣股票資訊，每日快取一次"""
        if not os.path.exists("cache"):
            os.makedirs("cache")
        files = os.listdir("cache")
        date = datetime.datetime.today().strftime("%Y-%m-%d")
        fileName = f"{date}_TaiwanInfo.pkl"
        if fileName in files:
            print("當日以查詢")
            df = pd.read_pickle("cache/" + fileName)
            return df
        else:
            if self.api is None:
                return None
            df = self.api.taiwan_stock_info()
            df.to_pickle("cache/" + fileName)
            return df

    def getLatestTradingDate(self):
        """取得最新的交易日期"""
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        dateDf = self.api.taiwan_stock_trading_date(end_date=today)
        return dateDf.tail(1)["date"].iloc[0]


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


if __name__ == "__main__":
    api = FinMindApi()
    startDate = "1994-10-01"
    latestTradingDate = api.getLatestTradingDate()
    latestCacheDate = getDataDate()
    print(f"最新的交易日期是: {latestTradingDate}")
    print(f"最新的快取日期是: {latestCacheDate}")

    if latestCacheDate != latestTradingDate:
        print("快取資料不是最新的，將從 API 取得最新資料")
        if os.path.exists("data"):
            os.removedirs("data")
        getAllTaiwanStockInfo = api.getAllTaiwanStockInfo()
        if getAllTaiwanStockInfo is None:
            print("無法取得台灣股票資訊")
            exit()
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
    else:
        print("快取資料已經是最新的，直接使用快取資料")
        files = os.listdir("data")
        print(f"共處理了 {len(files)} 檔股票資料")
