import os
import pandas as pd
import numpy as np


def calc_weighted_score(close: np.ndarray) -> np.ndarray:
    """
    對單一股票的 close 價格序列，計算每天的 weightedScore
    使用：
    3m = 63 天
    6m = 126 天
    9m = 189 天
    12m = 252 天
    """
    n = close.shape[0]
    weighted = np.full(n, np.nan, dtype=np.float32)
    if n <= 252:
        return weighted

    tail = close[252:]
    with np.errstate(divide="ignore", invalid="ignore"):
        weighted[252:] = (
            (tail / close[189:-63] - 1.0) * 0.4 +
            (tail / close[126:-126] - 1.0) * 0.2 +
            (tail / close[63:-189] - 1.0) * 0.2 +
            (tail / close[:-252] - 1.0) * 0.2
        ).astype(np.float32, copy=False)

    return weighted


def caculate_delta_rs(rs):


def build_features_from_close(close: np.ndarray) -> dict[str, np.ndarray]:
    close_series = pd.Series(close, copy=False)

    return {
        "roc5": close_series.pct_change(5).to_numpy(dtype=np.float32),
        "roc20": close_series.pct_change(20).to_numpy(dtype=np.float32),
        "ma5": close_series.rolling(5).mean().to_numpy(dtype=np.float32),
        "ma20": close_series.rolling(20).mean().to_numpy(dtype=np.float32),
        "volatility": close_series.pct_change().rolling(5).std().to_numpy(dtype=np.float32),
    }


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

        if len(df) <= 252:
            continue

        # 只在真的沒排序時才排序
        if not df.index.is_monotonic_increasing:
            df = df.sort_index()

        close_np = df["close"].to_numpy(dtype=np.float64, copy=False)
        weighted_score = calc_weighted_score(close_np)
        valid_mask = ~np.isnan(weighted_score)
        if not valid_mask.any():
            continue

        features = build_features_from_close(close_np)

        date_arr = df["date"].to_numpy()
        entry_date = np.empty(date_arr.shape[0], dtype=object)
        entry_date[:-1] = date_arr[1:]
        entry_date[-1] = pd.NaT

        open_arr = df["open"].to_numpy(dtype=np.float32, copy=False)
        entry_price = np.empty(open_arr.shape[0], dtype=np.float32)
        entry_price[:-1] = open_arr[1:]
        entry_price[-1] = np.nan

        # 僅保留 weightedScore 有效資料列，減少 concat 與 groupby 成本
        temp = pd.DataFrame({
            "stock_id": df["stock_id"].iloc[0],
            "date": date_arr[valid_mask],
            "volume": df["Trading_Volume"].to_numpy(dtype=np.int32, copy=False)[valid_mask],
            "weightedScore": weighted_score[valid_mask],
            "close": close_np.astype(np.float32, copy=False)[valid_mask],
            "entryDate": entry_date[valid_mask],
            "entryPrice": entry_price[valid_mask],
            "roc20": features["roc20"][valid_mask],
            "roc60": features["roc60"][valid_mask],
            "ma5": features["ma5"][valid_mask],
            "ma20": features["ma20"][valid_mask],
            "volatility": features["volatility"][valid_mask],
        })
        all_stock_scores.append(temp)

    if not all_stock_scores:
        print("沒有可用資料")
        return pd.DataFrame()

    # 一次合併
    big_df = pd.concat(all_stock_scores, ignore_index=True)

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
