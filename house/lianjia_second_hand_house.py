# -*- coding: utf-8 -*-
import json
import urllib2
import datetime

from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from bs4 import BeautifulSoup

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

class DBConnection(object):
	def __init__(self, host, user, passwd, database, charset="utf8"):
		self.engine = create_engine('mysql://%s:%s@%s/%s?charset=%s'%(user, passwd, host, database, charset), echo=True)	#echo=True 表示用logging输出调试结果

Base = declarative_base()
class Selling(Base):		#在售房源
	__tablename__ = "second_hand_house_selling"	#创建的表的名称

	id 			= Column(Integer, primary_key=True)
	xingzhengqu	= Column(String(50))	#所属行政区
	url		 	= Column(String(200))	#链接的url链接
	xiaoqu		= Column(String(50))	#小区名
	huxing		= Column(String(50))	#户型，如两室两厅
	mianji		= Column(Float)			#面积
	chaoxiang	= Column(String(50))	#朝向
	zhuangxiu	= Column(String(50))	#装修水平
	dianti		= Column(String(50))	#电梯配置情况
	louceng		= Column(String(50))	#所处楼层	
	zonglouceng = Column(Integer)		#总楼层
	nianfen		= Column(Integer)		#建造年份
	bankuai		= Column(String(50))	#所属板块，如申花板块，桥西板块
	guanzhushu	= Column(Integer)		#链家网站上关注的人数
	kanfangcishu= Column(Integer)		#有多少人次看过此房源
	fabutianshu = Column(Integer)		#在链家网站上发布的天数
	zongjia		= Column(Float)			#房东挂牌总价
	danjia		= Column(Integer)		#折算单价

	def __init__(self, xingzhengqu, url, xiaoqu, huxing, mianji, chaoxiang,zhuangxiu, dianti, 
				louceng, zonglouceng, nianfen, bankuai, 
				guanzhushu, kanfangcishu, fabutianshu,
				zongjia, danjia):
		self.xingzhengqu = xingzhengqu
		self.url		 = url
		self.xiaoqu		 = xiaoqu
		self.huxing		 = huxing
		self.mianji		 = mianji
		self.chaoxiang	 = chaoxiang
		self.zhuangxiu	 = zhuangxiu
		self.dianti		 = dianti
		self.louceng	 = louceng
		self.zonglouceng = zonglouceng
		self.nianfen	 = nianfen
		self.bankuai	 = bankuai
		self.guanzhushu  = guanzhushu
		self.kanfangcishu= kanfangcishu
		self.fabutianshu = fabutianshu
		self.zongjia	 = zongjia
		self.danjia		 = danjia
		
		
class LianjiaSecondHouseInfo(object):
	def __init__(self, db_conn):
		self.db_conn = db_conn	#数据库的连接信息

		self.headers = {"Host":"hz.lianjia.com", "Connection":"keep-alive",}
		
		#创建数据库表，如果数据库中存在该表，则不会重复创建
		Base.metadata.create_all(self.db_conn.engine)

		#创建的数据操作的session
		Session = sessionmaker(bind=self.db_conn.engine)
		self.session = Session()

		self.xingzhengqu = {"xihu":"西湖", "gongshu":"拱墅", "shangcheng":"上城", 
							"xiacheng":"下城", "binjiang":"滨江", "jianggan":"江干", 
							"yuhang":"余杭", "xiaoshan":"萧山"}

	def collect_selling_data(self,):
		for xingzhengqu_en, xingzhengqu_cn in self.xingzhengqu.items():
			print "===当前处理的行政区为：%s==="%xingzhengqu_cn
			source_url = "http://hz.lianjia.com/ershoufang/%s/" % xingzhengqu_en
			request = urllib2.Request(source_url, headers=self.headers)
			response = urllib2.urlopen(request, timeout=10).read()
			soup = BeautifulSoup(unicode(response), "html.parser")

			#获得行政区内所有房屋信息的总页数
			page_info = soup.find("div", {"class":"page-box house-lst-page-box"})
			total_page = json.loads(page_info.get("page-data"))["totalPage"]

			#遍历所有页面内的房源内容
			for page_index in range(1, total_page+1):
				print "===当前处理第%s页==="%page_index 
				url = source_url + "/pg" + str(page_index) + "/"
				request = urllib2.Request(url, headers=self.headers)
				response = urllib2.urlopen(request, timeout=10).read()
				soup = BeautifulSoup(unicode(response), "html.parser")

				#遍历一个页面上的所有房源信息
				house_list = soup.findAll("div", {"class": "info clear"})
				for house in house_list:
					#小区名，链接
					house_title = house.find("div", {"class": "title"})
					href = house_title.a.get("href")
					xiaoqu = house_title.get_text().split(" ")[0].replace("\n","")
					print xiaoqu

					#户型，面积，朝向，装修，电梯等房屋基本信息
					house_info = house.find("div", {"class": "houseInfo"}).get_text()
					house_info_items = house_info.split("|")
					huxing = house_info_items[1].strip()
					try:
						mianji = float(house_info_items[2].strip()[:-2])
					except:
						mianji = -1 #在字符串中没有按指定格式列填写面积，导致无法处理

					chaoxiang = house_info_items[3].strip()
					
					try:
						zhuangxiu = house_info_items[4].strip()
					except:
						zhuangxiu = "无登记信息"

					try:
						dianti = house_info_items[5].strip()
					except:
						dianti = "无登记信息"

					#楼层，年份，板块信息
					position_info = house.find("div", {"class": "positionInfo"})

					position_info_items = [item for item in position_info.get_text().split(" ") if item!='' and item !='\n']
					print position_info_items
					if position_info_items[0].find("(")!=-1:
						louceng = position_info_items[0].split("(")[0].replace("\n","")
						zonglouceng = int(position_info_items[0].split("(")[1][1:-2])
						nianfen_index = 1
					else:					#有些没有填写楼层信息
						louceng = "暂无信息"
						zonglouceng = -1
						nianfen_index = 0
					try:
						nianfen = int(position_info_items[nianfen_index][:4])
					except:
						nianfen = -1		#有些没有登记年份信息

					bankuai = position_info.a.get_text()

					#关注，看房人数，发布时间信息
					follow_info = house.find("div", {"class": "followInfo"}).get_text()
					follow_info_items = follow_info.split("/")
					guanzhushu = int(follow_info_items[0].strip()[:-3])
					kanfangcishu = int(follow_info_items[1].strip()[1:-3])
					fabutianshu_str = follow_info_items[2].strip()
					if fabutianshu_str.find("天")!=-1:
						fabutianshu = int(fabutianshu_str[:-5])
					elif fabutianshu_str.find("月")!=-1:
						fabutianshu = int(fabutianshu_str[:-6])*30	#如果是发布月数，就按每月30天折算成总天数
					else:
						continue	#一年以上的发布信息暂时丢弃掉

					#挂牌总价与单价信息
					zongjia = float(house.find("div", {"class": "totalPrice"}).find("span").get_text())
					danjia	= int(house.find("div", {"class": "unitPrice"}).find("span").get_text()[2:-4])

					selling = Selling(xingzhengqu_cn, href, xiaoqu, huxing, mianji, chaoxiang,zhuangxiu, dianti, 
									louceng, zonglouceng, nianfen, bankuai, 
									guanzhushu, kanfangcishu, fabutianshu,
									zongjia, danjia)
					self.session.add(selling)
					self.session.commit()


if __name__ == '__main__':
	#数据库链接的基础配置信息，当前支持的数据库是mysql
	host = "127.0.0.1:3306"
	user = "root"
	passwd = "test"
	database = "data_analysis"	#数据库要自行创建，但是其中的表不需要创建，sqlalchemy会辅助自动创建
	db_conn = DBConnection(host, user, passwd, database)

	LianjiaSecondHouseInfo(db_conn).collect_selling_data()

