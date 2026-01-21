import requests
import json

# 你的飞书机器人Webhook URL
WEBHOOK_URL = 'https://open.feishu.cn/open-apis/bot/v2/hook/fd513b57-0502-43f6-b08e-2f5d7ed3b171'


def send_message(title, msg):
    headers = {
        'Content-Type': 'application/json'
    }
    # 消息内容
    message = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [
                        [
                            {
                                "tag": "text",
                                "text": msg
                            }
                        ]
                    ]
                }
            }
        }
    }

    response = requests.post(WEBHOOK_URL, headers=headers, data=json.dumps(message))

    if response.status_code == 200:
        print("Message sent successfully!")
    else:
        print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")


if __name__ == "__main__":
    send_message('this is title', 'this is message')
