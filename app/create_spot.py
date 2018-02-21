import pandas as pd
from sql_control import SqlControl

class Spot(SqlControl):
    def __init__(self):
        print("Start to create spot.")
        # server start
        #sf database
        SqlControl.__init__(self)
        SqlControl.open_sf_conn(self)
        #Commodity database
        SqlControl.open_commodity_conn(self)
        self.spot_tb_name = 'spot'
        #------------------------------------------------------------------------------------------
        sfTbList = self.table_list()  # table name list from sf database
        tbName = sfTbList[-1] # last table of sf
        spotList = self.get_spot_name(tbName)
        self.create_spot_tb(spotList, sfTbList)  # create the commodity db
        self.insert_commodity(spotList, sfTbList)
        self.storage_spot_to_excel()  #read spot db and to excel
        #------------------------------------------------------------------------------------------
        #close server
        SqlControl.close_commodity_conn(self)
        SqlControl.close_sf_conn(self)
        print("Spot table created!")

    #return table name string in list
    def table_list(self):
        tbList = self.sf_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'order by name;").fetchall()
        for i in range(len(tbList)):
            tbList[i] = tbList[i][0]
        return tbList

    def create_spot_tb(self, spotList, sfTbList):
        tbName = self.spot_tb_name # name of the table with all commodity.
        #check if table existed than delete the table
        if self.check_table(tbName):
            sqlDrop = "DROP TABLE {0};".format(tbName)
            self.com_cursor.execute(sqlDrop)
        # create an empty table only with id and date in string
        sqlCreate = "CREATE TABLE {0} ('日期' text primary key);".format(tbName)
        self.com_cursor.execute(sqlCreate)
        # use loop to add all spot into the table as field
        for s in spotList:
            sqlAddSpot = "ALTER table {0} add '{1}' real;".format(tbName, s)
            self.com_cursor.execute(sqlAddSpot)
        # insert the dateTime to the spot table as index, later for updating the table data with sql update wording
        for sf in sfTbList:
            # get table date time as string, in order to store into the spot list as index
            dateTime = sf.replace("sf", "")
            sqlDate = "INSERT INTO {0} ('日期') VALUES ({1});".format(tbName, dateTime)
            self.com_cursor.execute(sqlDate)

    def get_spot_name(self, tbName):
        #read the lastest database and return the name list of spot
        sql = "SELECT 商品 from {0};".format(tbName)
        spotList = self.sf_cursor.execute(sql).fetchall()
        for i in range(len(spotList)):
            spotList[i] = spotList[i][0]
        return spotList

    #check table existed
    def check_table(self, tbName):
        sqlCheck = "SELECT COUNT(*) from sqlite_master WHERE type= 'table' and name = '{0}';".format(tbName)
        self.com_cursor.execute(sqlCheck)
        #if table existed, than true.
        res = self.com_cursor.fetchone()
        if res == (1,):
            return True
        else:
            return False

    def insert_commodity(self, spotList, sfTbList):
        # loop for all sf tables
        for sf in sfTbList:
            # get table date time as string, in order to store into the spot list
            dateTime = sf.replace("sf", "")
            # loop for all spot in one table
            for sp in spotList:
                # get spot price from sf table
                # select 现货价格 from table sf in upper loop where 商品 = sp in lower loop
                sqlGet = "SELECT 现货价格 FROM {0} WHERE 商品  = '{1}'".format(sf, sp)
                spotPrice = self.sf_cursor.execute(sqlGet).fetchall()
                spotPrice = spotPrice[0][0]
                # insert price to spot table
                # inser into table spot (商品名称) values (商品价格) by update
                sqlInsert = "UPDATE {0} SET '{1}' = {2} WHERE 日期 = '{3}';".format(self.spot_tb_name, sp, spotPrice, dateTime)
                self.com_cursor.execute(sqlInsert)

    def storage_spot_to_excel(self):
        sqlReadSpot = "SELECT * FROM spot"
        dfContent = pd.read_sql(sqlReadSpot, self.com_conn)
        excel_path = "{0}/database/SpotData.xlsx".format(self.BASE_DIR)
        dfContent.to_excel(excel_path)


if __name__ == '__main__':
    a = Spot()
