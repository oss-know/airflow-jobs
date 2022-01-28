from multidict import CIMultiDict

def lib_demo_multidict():
    dic = CIMultiDict()
    dic["key"] = "不区分大小写的 COMPANY_COUNTRY"
    print(f'dic["KEy"]:{dic["KEy"]}')
    return dic["KEy"]
