import requests


class GetOrder2:
    def __init__(self):
        self.url1 = "https://erp.eworldtours.com/plugins/J2Travel.Order/getdata.aspx"
        self.url2 = "https://erp.eworldtours.com/TravelOptPage/system/CommOpt.aspx"
        self.response = None
        self.headers1 = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Cookie": "_gcl_au=1.1.260138684.1757388165; _ga=GA1.1.1543263768.1757388196; _ga_ME5JMBJSZJ=GS2.1.s1757466017$o4$g0$t1757467542$j60$l0$h0; ASP.NET_SessionId=fejfhmbut14njkfa0remmbkr; TESTCookieName=TEsTCookievalue; lan=cn; uinfo=yunlin=92860EF9DDCF6C798BB4D49B709421DF09B9BB242E3D783D061D2E7AA71364EF",
            "Host": "erp.eworldtours.com",
            "Pragma": "no-cache",
            "Referer": "https://erp.eworldtours.com/plugins/J2Travel.order/order/orderlist.aspx?lm=all&modelid=b426120c-ab3c-47fb-bca9-53d4bd3954b3&lan=cn",
            "Sec-Ch-Ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Microsoft Edge\";v=\"140\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "Sec-Fetch-Dest": "iframe",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
        }
        self.headers2 = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Cookie": "_gcl_au=1.1.260138684.1757388165; _ga=GA1.1.1543263768.1757388196; _ga_ME5JMBJSZJ=GS2.1.s1757466017$o4$g0$t1757467542$j60$l0$h0; ASP.NET_SessionId=fejfhmbut14njkfa0remmbkr; TESTCookieName=TEsTCookievalue; lan=cn; uinfo=yunlin=83D5C5C45BC659D4F247EAFFA5F9D8A7CC997023166BB636",
            "Host": "erp.eworldtours.com",
            "Pragma": "no-cache",
            "Referer": "https://erp.eworldtours.com/plugins/J2Travel.order/order/orderlist.aspx?lm=all&modelid=b426120c-ab3c-47fb-bca9-53d4bd3954b3&lan=cn",
            "Sec-Ch-Ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Microsoft Edge\";v=\"140\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "Sec-Fetch-Dest": "iframe",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
        }
        #------Eworld---------
        self.payload1 = "action=getorderlist&otype=&moduleId=b426120c-ab3c-47fb-bca9-53d4bd3954b3&print=export&type=all&iswc=&inpara=%5B%7B%27splm%27%3A%27all%27%2C%27tabsel%27%3A%27%27%2C%27cylx%27%3A%27%27%2C%27xlpp%27%3A%27%27%2C%27lm%27%3A%27%27%2C%27ywy%27%3A%27%27%2C%27xdbm%27%3A%27%27%2C%27xdr%27%3A%27%27%2C%27skbm%27%3A%27%27%2C%27gys%27%3A%27%27%2C%27xdsjs%27%3A%27%27%2C%27xdsje%27%3A%27%27%2C%27lczt%27%3A%27%27%2C%27ctrqs%27%3A%27%27%2C%27ctrqe%27%3A%27%27%2C%27xllx%27%3A%27%27%2C%27skqd%27%3A%27%27%2C%27ztwf%27%3A%27%27%2C%27strbqlx%27%3A%27%27%2C%27cyrq%27%3A%27%27%2C%27xcts%27%3A%27%27%2C%27xcte%27%3A%27%27%2C%27mdd%27%3A%27%27%2C%27cfd%27%3A%27%27%2C%27spm%27%3A%27%27%2C%27strbq%27%3A%27%27%2C%27order%27%3A%27null%20asc%27%2C%27ykmd%27%3A%27%27%2C%27wkdqtx%27%3A%27%27%7D%5D&timer=1757475471827"
        self.payload2 = "action=getinnerorderlist&otype=pt&moduleId=ff64aabc-43fa-4b38-972d-839287065f73&print=export&type=gty&iswc=&inpara=%5B%7B%27splm%27%3A%27gty%27%2C%27tabsel%27%3A%27%27%2C%27cylx%27%3A%27%27%2C%27xlpp%27%3A%27%27%2C%27lm%27%3A%27%27%2C%27ywy%27%3A%27%27%2C%27xdbm%27%3A%27%27%2C%27xdr%27%3A%27%27%2C%27skbm%27%3A%27%27%2C%27gys%27%3A%27%27%2C%27xdsjs%27%3A%27%27%2C%27xdsje%27%3A%27%27%2C%27lczt%27%3A%27%27%2C%27ctrqs%27%3A%27%27%2C%27ctrqe%27%3A%27%27%2C%27xllx%27%3A%27%27%2C%27skqd%27%3A%27%27%2C%27ztwf%27%3A%27%27%2C%27strbqlx%27%3A%27%27%2C%27cyrq%27%3A%27%27%2C%27xcts%27%3A%27%27%2C%27xcte%27%3A%27%27%2C%27mdd%27%3A%27%27%2C%27cfd%27%3A%27%27%2C%27spm%27%3A%27%27%2C%27strbq%27%3A%27%27%2C%27order%27%3A%27null%20asc%27%2C%27ykmd%27%3A%27%27%2C%27wkdqtx%27%3A%27%27%7D%5D&timer=1757481928017"
        self.payload3 = "action=getinnerorderlist&otype=dx&moduleId=5856bed2-7d97-4a9f-932a-b6e77e08fe36&print=export&type=gty&iswc=&inpara=%5B%7B%27splm%27%3A%27gty%27%2C%27tabsel%27%3A%27%27%2C%27cylx%27%3A%27%27%2C%27xlpp%27%3A%27%27%2C%27lm%27%3A%27%27%2C%27ywy%27%3A%27%27%2C%27xdbm%27%3A%27%27%2C%27xdr%27%3A%27%27%2C%27skbm%27%3A%27%27%2C%27gys%27%3A%27%27%2C%27xdsjs%27%3A%27%27%2C%27xdsje%27%3A%27%27%2C%27lczt%27%3A%27%27%2C%27ctrqs%27%3A%27%27%2C%27ctrqe%27%3A%27%27%2C%27xllx%27%3A%27%27%2C%27skqd%27%3A%27%27%2C%27ztwf%27%3A%27%27%2C%27strbqlx%27%3A%27%27%2C%27cyrq%27%3A%27%27%2C%27xcts%27%3A%27%27%2C%27xcte%27%3A%27%27%2C%27mdd%27%3A%27%27%2C%27cfd%27%3A%27%27%2C%27spm%27%3A%27%27%2C%27strbq%27%3A%27%27%2C%27order%27%3A%27null%20asc%27%2C%27ykmd%27%3A%27%27%2C%27wkdqtx%27%3A%27%27%7D%5D&timer=1757482304483"
        self.data = {
            "viewid": "undefined",
            "file": None,
            "opt": "SDownFile",
            "randomid": "8399_1757659897659"
        }
        #------Tinma-----------
        self.payload4 = "action=getorderlist&otype=&moduleId=b426120c-ab3c-47fb-bca9-53d4bd3954b3&print=export&type=all&iswc=&inpara=%5B%7B%27splm%27%3A%27all%27%2C%27tabsel%27%3A%27%27%2C%27cylx%27%3A%27%27%2C%27xlpp%27%3A%27%27%2C%27lm%27%3A%27%27%2C%27ywy%27%3A%27%27%2C%27xdbm%27%3A%27%27%2C%27xdr%27%3A%27%27%2C%27skbm%27%3A%27%27%2C%27gys%27%3A%27%27%2C%27xdsjs%27%3A%27%27%2C%27xdsje%27%3A%27%27%2C%27lczt%27%3A%27%27%2C%27ctrqs%27%3A%27%27%2C%27ctrqe%27%3A%27%27%2C%27xllx%27%3A%27%27%2C%27skqd%27%3A%27%27%2C%27ztwf%27%3A%27%27%2C%27strbqlx%27%3A%27%27%2C%27cyrq%27%3A%27%27%2C%27xcts%27%3A%27%27%2C%27xcte%27%3A%27%27%2C%27mdd%27%3A%27%27%2C%27cfd%27%3A%27%27%2C%27spm%27%3A%27%27%2C%27strbq%27%3A%27%27%2C%27order%27%3A%27null%20asc%27%2C%27ykmd%27%3A%27%27%2C%27wkdqtx%27%3A%27%27%7D%5D&timer=1757660715566"
        self.payload5 = "action=getinnerorderlist&otype=pt&moduleId=ff64aabc-43fa-4b38-972d-839287065f73&print=export&type=gty&iswc=&inpara=%5B%7B%27splm%27%3A%27gty%27%2C%27tabsel%27%3A%27%27%2C%27cylx%27%3A%27%27%2C%27xlpp%27%3A%27%27%2C%27lm%27%3A%27%27%2C%27ywy%27%3A%27%27%2C%27xdbm%27%3A%27%27%2C%27xdr%27%3A%27%27%2C%27skbm%27%3A%27%27%2C%27gys%27%3A%27%27%2C%27xdsjs%27%3A%27%27%2C%27xdsje%27%3A%27%27%2C%27lczt%27%3A%27%27%2C%27ctrqs%27%3A%27%27%2C%27ctrqe%27%3A%27%27%2C%27xllx%27%3A%27%27%2C%27skqd%27%3A%27%27%2C%27ztwf%27%3A%27%27%2C%27strbqlx%27%3A%27%27%2C%27cyrq%27%3A%27%27%2C%27xcts%27%3A%27%27%2C%27xcte%27%3A%27%27%2C%27mdd%27%3A%27%27%2C%27cfd%27%3A%27%27%2C%27spm%27%3A%27%27%2C%27strbq%27%3A%27%27%2C%27order%27%3A%27null%20asc%27%2C%27ykmd%27%3A%27%27%2C%27wkdqtx%27%3A%27%27%7D%5D&timer=1757494449307"
        self.payload6 = "action=getinnerorderlist&otype=dx&moduleId=5856bed2-7d97-4a9f-932a-b6e77e08fe36&print=export&type=gty&iswc=&inpara=%5B%7B%27splm%27%3A%27gty%27%2C%27tabsel%27%3A%27%27%2C%27cylx%27%3A%27%27%2C%27xlpp%27%3A%27%27%2C%27lm%27%3A%27%27%2C%27ywy%27%3A%27%27%2C%27xdbm%27%3A%27%27%2C%27xdr%27%3A%27%27%2C%27skbm%27%3A%27%27%2C%27gys%27%3A%27%27%2C%27xdsjs%27%3A%27%27%2C%27xdsje%27%3A%27%27%2C%27lczt%27%3A%27%27%2C%27ctrqs%27%3A%27%27%2C%27ctrqe%27%3A%27%27%2C%27xllx%27%3A%27%27%2C%27skqd%27%3A%27%27%2C%27ztwf%27%3A%27%27%2C%27strbqlx%27%3A%27%27%2C%27cyrq%27%3A%27%27%2C%27xcts%27%3A%27%27%2C%27xcte%27%3A%27%27%2C%27mdd%27%3A%27%27%2C%27cfd%27%3A%27%27%2C%27spm%27%3A%27%27%2C%27strbq%27%3A%27%27%2C%27order%27%3A%27null%20asc%27%2C%27ykmd%27%3A%27%27%2C%27wkdqtx%27%3A%27%27%7D%5D&timer=1757494573772"
        self.data2 = {
            "viewid": "undefined",
            "file": None,
            "opt": "SDownFile",
            "randomid": "1926_1757660748050"
        }
    def get(self,filename,payload,headers,data):
        response1 = requests.post(self.url1, headers=headers, params=payload)
        self.data2["file"]= response1.text
        print(response1.text)
        response2 = requests.post(self.url2, headers=headers, data=data)
        with open(filename, 'wb') as f:
            f.write(response2.content)
        print(f"文件已保存为: {filename}")
if __name__ == '__main__':
    pass

