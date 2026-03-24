from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# 定義資料模型，使用小駝峰命名


class UserProfile(BaseModel):
    userId: int
    userName: str
    emailAddress: str


@app.get("/api/userProfile/{userId}", response_model=UserProfile)
async def getUserProfile(userId: int):
    # 模擬資料庫查詢
    userData = {
        "userId": userId,
        "userName": "johndoe",
        "emailAddress": "john@example.com"
    }
    return userData
