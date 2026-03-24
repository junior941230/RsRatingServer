import os
import pandas as pd
from datetime import datetime


def calc_weighted_score(close: pd.Series) -> pd.Series:
    """
    對單一股票的 close 價格序列，計算每天的 weightedScore
    使用：
    3m = 63 天
    6m = 126 天
    9m = 189 天
    12m = 252 天
    """
    roc3m = close / close.shift(63) - 1
    roc6m = close / close.shift(126) - 1
    roc9m = close / close.shift(189) - 1
    roc12m = close / close.shift(252) - 1

    weighted_score = (
        roc3m * 0.4 +
        roc6m * 0.2 +
        roc9m * 0.2 +
        roc12m * 0.2
    )

    return weighted_score


def caculate_rs_rating():
    """計算每天所有股票的 RS Rating，並存成 daily_rs_table.pkl"""
    all_stock_scores = []
    files = os.listdir("data")

    for file in files:
        if len(file) != 30:
            continue

        path = os.path.join("data", file)
        df = pd.read_pickle(path)

        if len(df) < 252:
            continue

        df = df.copy()
        df = df.sort_index()  # 確保時間順序正確

        stock_id = df["stock_id"].iloc[0]

        # 算每天的 weightedScore
        df["weightedScore"] = calc_weighted_score(df["close"])

        # 只保留需要欄位
        temp = df[["stock_id", "weightedScore"]].copy()
        temp["stock_id"] = stock_id
        temp["date"] = df["date"]

        all_stock_scores.append(temp)

    # 合併所有股票
    big_df = pd.concat(all_stock_scores, ignore_index=True)

    # 移除還沒滿一年、算不出來的日期
    big_df = big_df.dropna(subset=["weightedScore"])

    # 同一天所有股票互相比排名，算 RS
    big_df["rsRating"] = (
        big_df.groupby("date")["weightedScore"]
        .rank(pct=True) * 100
    ).astype(int)

    # 排序
    big_df = big_df.sort_values(["date", "rsRating"], ascending=[True, False])

    # 存檔
    today = datetime.now().strftime("%Y-%m-%d")
    big_df.to_pickle(f"cache/{today}_RS.pkl")


if __name__ == "__main__":
    caculate_rs_rating()
