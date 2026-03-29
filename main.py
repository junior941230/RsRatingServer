from dataGet import getAllHistoryAdjustedPrices
from RSRating import calculateRsRating
import time
if __name__ == "__main__":
    # getAllHistoryAdjustedPrices()
    start_time = time.time()
    rsRating = calculateRsRating()
    print(f"RS Rating 計算完成，耗時 {time.time() - start_time:.2f} 秒")
