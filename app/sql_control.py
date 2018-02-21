import settings
import sqlite3

class SqlControl:

    def __init__(self):
        self.BASE_DIR = settings.base_dir()    #Futures_Web DIR path
        self.sfdata_db_path = self.BASE_DIR + '/database' + '/Sfdata.db'      # database path of sf data
        self.commodity_db_path = self.BASE_DIR + '/database' + '/Commodity.db'  # database path of commodity

    def open_sf_conn(self):
        self.sf_conn = sqlite3.connect(self.sfdata_db_path)   #conn of sfdata open
        self.sf_cursor = self.sf_conn.cursor()

    def close_sf_conn(self):
        self.sf_conn.commit()    #commit sfdata
        self.sf_conn.close()     #conn of sfdata close

    def open_commodity_conn(self):
        self.com_conn = sqlite3.connect(self.commodity_db_path)   #conn of sfdata open
        self.com_cursor = self.com_conn.cursor()

    def close_commodity_conn(self):
        self.com_conn.commit()    #commit sfdata
        self.com_conn.close()     #conn of sfdata close


if __name__ == "__main__":
    test = SqlControl()
    test.open_commodity_conn()
    test.open_sf_conn()
    test.close_commodity_conn()
    test.close_sf_conn()