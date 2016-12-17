#encoding=UTF-8
import requests
import json

import datetime
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class DBConnection(object):
	def __init__(self, host, user, passwd, database, charset="utf8"):
		self.engine = create_engine('mysql://%s:%s@%s/%s?charset=%s'%(user, passwd, host, database, charset), echo=True)	#echo=True 表示用logging输出调试结果

Base = declarative_base()
class Position(Base):
	__tablename__ = "position"	#创建的表的名称

	id = Column(Integer, primary_key=True)
	position_name = Column(String(50))
	work_year = Column(String(50))
	education = Column(String(50))
	salary_ll = Column(Integer)
	salary_ul = Column(Integer)
	city = Column(String(50))
	district = Column(String(50))
	finance_stage = Column(String(500))
	industry_field = Column(String(500))
	company_short_name = Column(String(500))
	company_size = Column(String(50))

	def __init__(self, position_name, work_year, education, salary_ll, salary_ul,
				city, district, finance_stage, industry_field, 
				company_short_name, company_size):
		self.position_name = position_name
		self.work_year = work_year
		self.education = education
		self.salary_ll = salary_ll
		self.salary_ul = salary_ul
		self.city = city
		self.district = district
		self.finance_stage = finance_stage
		self.industry_field = industry_field
		self.company_short_name = company_short_name
		self.company_size = company_size

class LagouJobDeail(object):
	def __init__(self, db_conn, position_keyword, xueli="本科,硕士"):
		self.db_conn = db_conn	#数据库的连接信息

		self.xueli = xueli	#学历暂定为这两种
		self.url = "https://www.lagou.com/jobs/positionAjax.json?xl=%s&px=default&needAddtionalResult=false"%self.xueli
		self.headers = {"Host":"www.lagou.com", "Connection":"keep-alive"}
		self.position_keyword = position_keyword #要查询的职位关键信息
		self.page_size = None 	#每个页面显示的条目数
		self.last_page_size = None #最后一个页面显示的条目数
		self.page_total_count = None 	#总的页面数量
		self.page_index = 1		#初始的页面id
		self.form_data = {"first":"false","pn":self.page_index,"kd":self.position_keyword}	#post表单的附带参数

		#下面部分初始化基础信息
		response = requests.post(self.url,headers=self.headers,data=self.form_data)
		result = json.loads(response.text)
		success = result['success']
		self.page_size = result['content']['pageSize'] 						
		total_count = result['content']['positionResult']['totalCount']
		print "一共有%s行数据等待处理"%total_count
		self.page_total_count = total_count/self.page_size + 1
		self.last_page_size = total_count%self.page_size

		#创建数据库表，如果数据库中存在该表，则不会重复创建
		Base.metadata.create_all(self.db_conn.engine)

		#创建的数据操作的session
		Session = sessionmaker(bind=self.db_conn.engine)
		self.session = Session()


	def collect_job_data(self,):
		for i in range(self.page_index, self.page_total_count+1):
			self.form_data["pn"] = i
			response = requests.post(self.url,headers=self.headers,data=self.form_data)
			if response.text.find("404 Not Found")!=-1:
				continue
			content = json.loads(response.text)["content"]
			position_items = content["positionResult"]["result"]	#每个页面上所有的职位信息
			
			#处理最后一页的数据，可能行数是不满page_size的，则将page_size修改为当前业实际的行数
			if i==self.page_total_count: 
				self.page_size = self.last_page_size
			for j in range(0, self.page_size):
				salary_interval = position_items[j]["salary"]
				salary_interval_items=salary_interval.split("-")
				if len(salary_interval_items)==2:
					salary_interval_low_limit = int(salary_interval_items[0][:-1])		#去掉末尾的k字符
					salary_interval_high_limit = int(salary_interval_items[1][:-1])		#去掉末尾的k字符
				else:		#处理部分只有薪资下限的情况，如“20K以上”这种
					salary_interval_low_limit = int(salary_interval.split("k")[0])
					salary_interval_high_limit = int(salary_interval.split("k")[0])		#因为没有上限，暂时将上限设置为和下限一样
				position = Position(position_items[j]["positionName"],
									position_items[j]["workYear"],
									position_items[j]["education"],
									salary_interval_low_limit,
									salary_interval_high_limit,
									position_items[j]["city"],
									position_items[j]["district"],
									position_items[j]["financeStage"],
									position_items[j]["industryField"],
									position_items[j]["companyShortName"],
									position_items[j]["companySize"],)
				self.session.add(position)
				self.session.commit()

if __name__ == '__main__':
	#数据库链接的基础配置信息，当前支持的数据库是mysql
	host = "127.0.0.1:3306"
	user = "root"
	passwd = "test"
	database = "data_analysis"	#数据库要自行创建，但是其中的表不需要创建，sqlalchemy会辅助自动创建
	db_conn = DBConnection(host, user, passwd, database)

	#需要搜索的职位关键字
	position_keyword = "数据分析"

	#可供选择的学历为：博士、硕士、本科、专科，学历之间用英文逗号分开
	#xueli为空，表示不对学历进行筛选。
	#不传入xueli参数，默认是本科与硕士
	xueli = ""
	# for i in range(1,100):
	LagouJobDeail(db_conn, position_keyword, xueli).collect_job_data()
	print "完成"

