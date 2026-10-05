from config import USERS_CONFIG
from main import send_email

import asyncio

if __name__ == "__main__":
    # 创建一个简单的测试邮件内容
    with open("report.md", "r", encoding="utf-8") as f:
        body = f.read()

    receiver = USERS_CONFIG[0]["email"]

    # 异步发送邮件
    result = asyncio.run(send_email("每日ArXiv论文报告", body, receiver))

    if result:
        print("邮件发送成功")
    else:
        print("邮件发送失败")