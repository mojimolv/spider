#encoding=UTF-8
import requests
import time
from HTMLParser import HTMLParser

'''
这个文件主要用于从网站上爬取每天最新的交易数据到数据库中
'''

def get_daily_page():
    html_page=""
    headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Encoding":"gzip, deflate, sdch",
                "Accept-Language":"zh-CN,zh;q=0.8",
                "Cache-Control":"max-age=0",
                "Connection":"keep-alive",
                "Host":"www.tmsf.com",
                "Referer":"http://www.tmsf.com/daily.htm",
                "Upgrade-Insecure-Requests":"1",
                "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.89 Safari/537.36"
                }

    response=requests.get("http://www.tmsf.com/daily.htm",headers=headers)
    if response.status_code == 200:
        #如果采用response.text的方式，将会结果读不完全
        for chunk in response.iter_content(chunk_size=1024): 
            if chunk:
                html_page = html_page + chunk
        print html_page
        return html_page
    else:
        raise Exception("【异常】:获取网页内容失败，失败码："+str(response.status_code))
        
    
class DataHTMLParser(HTMLParser):
    def __init__(self):   
        HTMLParser.__init__(self)
        self.is_table = False
        self.is_tr = False
        self.is_td = False
        self.is_num_span = False
        self.row = []   #单个表行的数据
        self.rows = []  #所有表行的数据
        self.str_num = []
        self.current_area = "主城区"
        
        #便于后面单词和数字进行映射
        self.values={"numbone":"1",
                     "numbtwo":"2",
                     "numbthree":"3",
                     "numbfour":"4",
                     "numbfive":"5",
                     "numbsix":"6",
                     "numbseven":"7",
                     "numbeight":"8",
                     "numbnine":"9",
                     "numbzero":"0",
                     "numbdor":".",}
        
        #定义几个城区之间的顺序
        self.current_next_area={"主城区":"萧山",
                                "萧山":"余杭",
                                "余杭":"富阳",
                                "富阳":"大江东",
                                "大江东":""}
        
    def handle_starttag(self, tag, attrs):
        if tag == "table" and len(attrs)==3:    #拥有当天数据的表格刚好是以三个属性开始
            self.is_table = True
        
        if self.is_table and tag == "tr":
            self.is_tr = True
            self.row =[]    #读到新的一个表行，将表行数据重置
            
        if self.is_table and self.is_tr and tag == "td":
            self.is_td = True
            
        if self.is_table and self.is_tr and self.is_td and tag=="span": 
            #关于数字方面，该网站做法比较奇葩，数字没有直接显示出来
            self.is_num_span = True
            self.str_num.append(self.values.get(attrs[0][1]))
            
    def handle_data(self, data):
        if self.is_table and self.is_tr and self.is_td:
            if data.strip()!="":
                self.row.append(data.strip())
        
    def handle_endtag(self, tag):
        if tag == "table":
            self.is_table = False
        
        if self.is_table and tag == "tr":
            self.is_tr = False
            
            #去掉表的头尾
            if not self.row[0].startswith("楼盘名称") and not self.row[0].startswith("总计签约"):
                self.row.append(self.current_area)  #在行的末尾加上属于哪个城区
                self.rows.append(self.row)
                 
            if self.row[0].startswith("总计签约"):
                self.current_area = self.current_next_area.get(self.current_area)   #切换城区
            
        if self.is_table and self.is_tr and tag == "td":
            self.is_td = False
            self.row.append("".join(self.str_num))
            self.str_num=[]
            
        if self.is_table and self.is_tr and self.is_td and tag == "span":
            self.is_num_span = False


if __name__=="__main__":
    dhp = DataHTMLParser()
    print "【信息】：读取网页内容"
    page_html= get_daily_page()
    print "【信息】：开始解析网页内容"
    dhp.feed(page_html.replace("元/㎡", "").replace("㎡", ""))
    dhp.close()
    print "【信息】：开始将解析结果写入到文件中"
    today=time.strftime("%Y-%m-%d",time.localtime())
    f=file("/home/pi/DA/housedata/"+today,'w')
    for row in dhp.rows:
        #将两行没有用的内容删除掉，看着像空格，但是不是空格，暂时不清楚是什么，不然可以用remove函数一次性去除
        del row[1]
        del row[2]
        f.write(",".join(row)+"\n")
    f.flush()
    f.close()
    print "【信息】：完成所有操作"
    