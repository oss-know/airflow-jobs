import requests
import datetime
import pytz


def getConnectDemo(token):
    url = "http://api.github.com/rate_limit"
    headers = headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
        'Authorization': 'token ' + token
    }
    response = requests.get(url=url, headers=headers)
    result = response.json()
    # print(result)
    if response.status_code >= 400:
        print("本token为无效token")
        print(f'响应的结果:{result}')
    else:
        print(f"总限制:{result['resources']['core']['limit']}")
        print(f"使用了:{result['resources']['core']['used']}")
        print(f"剩余:{result['resources']['core']['remaining']}")

        t = datetime.datetime.fromtimestamp(int(result['resources']['core']['reset']),
                                            pytz.timezone('Asia/Shanghai')).strftime(
            '%Y-%m-%d %H:%M:%S')
        print(f"token限制重置时间:{t}")

    print(result)


tokens = [
]

for token in tokens:
    print("---------------------------------------------------")
    print(token)
    getConnectDemo(token)



