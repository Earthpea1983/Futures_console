from datetime import datetime
from pandas import date_range
import sqlite3
import requests as rq
from lxml import etree
import numpy as np
import pandas as pd
from numpy.random import randint
from time import sleep
from sql_control import SqlControl


class Crawler(SqlControl):

    def __init__(self):
        print("Start to crawl.")
        SqlControl.__init__(self)
        SqlControl.open_sf_conn(self)  # open database
        dateList = self.create_datelist()   #  create date list since 2017-10-9
        dateList = self.modify_datelist(dateList)  #  return dateList except already in sf database
        urlList = self.create_urllist(dateList)   #turn the date list to url list of 100ppi website
        self.crawl(urlList, dateList)
        SqlControl.close_sf_conn(self)  # subbmit and close database
        print("Data downloaded!")

    #create table with tbName
    def create_table(self, tbName):
        sqlCreate = "CREATE TABLE {0} (id int primary key, '商品' text,'现货价格' text, '代码' text, \
                '期货价格' real, '最高基差180' real, '最低基差180' real, '平均基差180' real, '基差' real,\
                 '极限180' real, '综合指标' real);".format(tbName)
        self.sf_cursor.execute(sqlCreate)

    #write dfContent (dataframe) to sql
    def write_table(self, conn, tbName, dfContent):
        dfContent.to_sql(tbName, conn, if_exists='replace')

    def create_datelist(self):
        beginDate = '2017-10-09'
        endDate = str(datetime.today().date())
        #create date list by pandas and reform to datetime format.
        dateList = [datetime.strftime(x, '%Y-%m-%d') for x in list(date_range(start=beginDate, end=endDate))]
        return dateList

    def check_latest(self):   #  return the lastest sftable name and return in time form eg.2017-01-01
        sql = "SELECT name FROM sqlite_master WHERE TYPE = 'table' ORDER BY name"
        tableList = self.sf_cursor.execute(sql).fetchall()
        if tableList != []:  # if tableList is empty, means first crawl, then pass the modify of dateList
            latestTable = tableList[-1][0]
            latestTable = latestTable[2:6]+'-'+latestTable[6:8]+'-'+latestTable[8::]
        else:
            latestTable = ''
        return latestTable

    def modify_datelist(self, dateList):  #  return the dateList since the lastest sfdate till present
        lastTable = self.check_latest()
        if lastTable != '':     #  lastTable=='' means first time crawl, then not modity the dateList
            ind = dateList.index(lastTable) + 1
            dateList = dateList[ind::]   #  from lastTable name to present
        return dateList

    def create_urllist(self, dateList):
        urlList = list(map(lambda x: "http://www.100ppi.com/sf2/day-"+ x + ".html", dateList))
        return urlList

    def parse(self, url):
        page = etree.HTML(rq.get(url).text)  #get the page html
        content = page.xpath("//td[a]/a/text()|//td[a]/../td/text()") # get page content, product name and product data
        for i in range(len(content)):
            content[i] = "".join(content[i].strip())     #loop the list, if there is \r \t \n \xa0, then use strip to delete and join in ""
        for i in range(content.count('')):  #count how many '' in list
            content.remove('')  # delete the '' in list
        return content   #return the date and content

    def check_empty(self, url):
        page = etree.HTML(rq.get(url).text)  # get the page html
        content = page.xpath("//table[@class='ftab']//td//text()")  # get page content, for the "暂无数据"
        if "暂无数据" in content:   #if text in table including "暂无数据" then return true
            return True
        else:
            return False

    def reshape(self, content):
        numofspot = 45  # numbers of spot
        cellofspot = numofspot*7  # each spot with 7 columns
        # 临时改变下载的数据，因绵纱数据缺失
        #——————————————————————
        for i in range(3):
            content.insert(207, 0.01)
        #——————————————————————
        content = content[0:cellofspot]  # 44 spot each with 7 column, others delete
        content = np.array(content)
        content = np.reshape(content, [numofspot, 7])
        dfContent = pd.DataFrame(content, columns=['商品','现货价格','代码','期货价格','最高基差180', \
                                                  '最低基差180','平均基差180'])
        dfContent['现货价格'] = dfContent['现货价格'].astype(float)
        dfContent['期货价格'] = dfContent['期货价格'].astype(float)
        dfContent['最高基差180'] = dfContent['最高基差180'].astype(float)
        dfContent['最低基差180'] = dfContent['最低基差180'].astype(float)
        dfContent['平均基差180'] = dfContent['平均基差180'].astype(float)
        return dfContent

    # Recalibrate the dataframe, and create one more "综合指标" column


    def process_df_content(self, dfContent):
        index = dfContent[dfContent.ix[:,"商品"]=="鸡蛋"].index.tolist() #get the 鸡蛋所在行
        dfContent.ix[index[0],1] *= 500  #鸡蛋单元换算成期货
        index = dfContent[dfContent.ix[:,"商品"]=="玻璃"].index.tolist() #get the 玻璃所在行
        dfContent.ix[index[0],1] *= 80  #玻璃单元换算成期货
        #cal the jicha
        jc = pd.DataFrame((dfContent["现货价格"]-dfContent["期货价格"])/dfContent["现货价格"]*100,columns=['基差'])
        dfContent = pd.concat([dfContent,jc],axis=1) #concat the result of jc to the dataframe
        #cal the limitation to the 180 avr
        limitation = np.array(np.zeros(len(dfContent["现货价格"]))) # use np array for append
        for i in range(len(dfContent["现货价格"])):       #loop to see if the jicha is possity or negative to define the divide
            if dfContent.ix[i,"基差"] < 0:
                limitation[i] = (dfContent.ix[i,"现货价格"]-dfContent.ix[i,"期货价格"])/dfContent.ix[i,"最低基差180"]
            else:
                limitation[i] = (dfContent.ix[i,"现货价格"]-dfContent.ix[i,"期货价格"])/dfContent.ix[i,"最高基差180"]
        limitation = pd.DataFrame(limitation,columns = ['极限180'])
        dfContent = pd.concat([dfContent,limitation],axis=1)  #concat the limitation to dataframe
        # cal the reference jicha, use jicha% * limiation
        ref_jc = pd.DataFrame(dfContent["基差"]*dfContent["极限180"],columns=['综合指标'])
        dfContent = pd.concat([dfContent,ref_jc],axis=1) #concat the result of jc to the dataframe
        return dfContent

    def get_single_page(self, url):
        content = self.parse(url)  #return the page table and date (used as table name) from the page
        dfContent = self.reshape(content)   #turn the table into pretty pandas dataframe and prepare for db
        dfContent = self.process_df_content(dfContent)
        return dfContent

    def randsleep(self):
        timeFrom = 5
        timeTo = 20
        sleep(randint(timeFrom, timeTo))

    def crawl(self, urlList, dateList):
        print("Ready to crawl!")
        for ind in range(len(urlList)):
            # time interval to avoid being ban by website.
            if ind != 0:  #except the first crawl
                self.randsleep()
            print("Crawling:\t" + urlList[ind] + "\t---->\t", end='')  #print the url line, not new line next
            # if page empty then pass
            if self.check_empty(urlList[ind]):  # page with "暂无数据", then next url
                print("PASS.")
                continue
            tbName = 'sf' + dateList[ind].replace('-', '')  # turn the date form '2017-10-09' to 'sf20171009' as table name
            dfContent = self.get_single_page(urlList[ind])  # get content in dataframe
            #no need to check sqlite talbe if not exists, due to the datelist already check
            self.create_table(tbName)  # create table
            self.write_table(self.sf_conn, tbName, dfContent)  #anyway, write the table, if already existed, then replace.
            #print out process
            print("Complete.")  # follow the print out in the beginning of loop.

if __name__ == "__main__":
    c = Crawler()