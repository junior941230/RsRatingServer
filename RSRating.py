import os
import pandas as pd
import numpy as np


def buildFeatures(df):
    df["roc20"] = df["close"].pct_change(5)  # 5天的報酬率
    df["roc60"] = df["close"].pct_change(20)  # 20天的報酬率

    df["ma5"] = df["close"].rolling(5).mean()  # 5天的移動平均線
    df["ma20"] = df["close"].rolling(20).mean()  # 20天的移動平均線

    df["volatility"] = df["close"].pct_change().rolling(5).std()  # 5天的波動率

    return df


def calc_weighted_score(close: pd.Series) -> pd.Series:
    """
    對單一股票的 close 價格序列，計算每天的 weightedScore
    使用：
    3m = 63 天
    6m = 126 天
    9m = 189 天
    12m = 252 天
    """
    roc3m = close / close.shift(63) - 1.0
    roc6m = close / close.shift(126) - 1.0
    roc9m = close / close.shift(189) - 1.0
    roc12m = close / close.shift(252) - 1.0

    return (
        roc3m * 0.4 +
        roc6m * 0.2 +
        roc9m * 0.2 +
        roc12m * 0.2
    )


def calculateRsRating():
    """計算每天所有股票的 RS Rating，並存成 cache/{today}_RS.pkl"""
    all_stock_scores = []

    # 用 scandir 比 listdir 更有效率
    for entry in os.scandir("data"):
        if not entry.is_file():
            continue

        file_name = entry.name
        if len(file_name) != 30:
            continue

        df = pd.read_pickle(entry.path)

        if len(df) < 252:
            continue

        # 只在真的沒排序時才排序
        if not df.index.is_monotonic_increasing:
            df = df.sort_index()

        close = df["close"]
        weighted_score = calc_weighted_score(close)

        # 直接建最小欄位，避免多餘 copy
        temp = pd.DataFrame({
            "stock_id": df["stock_id"].iloc[0],
            "date": df["date"].to_numpy(),
            "volume": df["Trading_Volume"].to_numpy(dtype=np.int32),
            "weightedScore": weighted_score.to_numpy(dtype=np.float32),
            "close": close.to_numpy(dtype=np.float32),
            "entryDate": df["date"].shift(-1).to_numpy(),
            "entryPrice": df["open"].shift(-1).to_numpy(dtype=np.float32),
        })
        temp = buildFeatures(temp)
        all_stock_scores.append(temp)

    if not all_stock_scores:
        print("沒有可用資料")
        return pd.DataFrame()

    # 一次合併
    big_df = pd.concat(all_stock_scores, ignore_index=True)

    # 去除無法計算 weightedScore 的資料
    big_df = big_df.dropna(subset=["weightedScore"])

    # 同一天排名
    big_df["rsRating"] = (
        big_df.groupby("date", sort=False)["weightedScore"]
        .rank(pct=True, method="min")
        .mul(100)
        .astype(np.uint8)
    )

    # 排序
    big_df = big_df.sort_values(["date", "rsRating"], ascending=[
                                True, False], kind="mergesort")

    # 存檔
    os.makedirs("cache", exist_ok=True)
    today = big_df["date"].max()
    big_df.to_pickle(f"cache/{today}_RS.pkl")

    return big_df


if __name__ == "__main__":
    calculateRsRating()
