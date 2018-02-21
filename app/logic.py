import sqlite3
import pandas as pd
import settings
from sql_control import SqlControl


class ThirtyThree(SqlControl):
    def __init__(self):
        print("Suggestion on the way.")
        SqlControl.__init__(self)
        BASE_DIR = settings.base_dir()
        self.excel_path = "{0}/database/Suggestion.xlsx".format(BASE_DIR)
        day = 33 # to get the 33day for calculation
        #sql start
        SqlControl.open_commodity_conn(self)
        SqlControl.open_sf_conn(self)
        #----------------------------------------------------------------------------------------------------------
        spot = self.get_spot()
        res = self.judge_spot(spot, day)  # -1 for going down, 1 for up, 0 for ignore
        df_content = self.get_sf()      #get sf table content
        res = self.refromat_res(res, df_content)  # reformat the res with jicha
        self.storage_to_excel(res)
        #-----------------------------------------------------------------------------------------------------------
        #sql stop
        SqlControl.close_sf_conn(self)
        SqlControl.close_commodity_conn(self)
        print("Suggestion has been made!")
        print("="*20)
        print(res)
        print("="*20)

    def get_spot(self):
        sql = "SELECT * FROM spot"
        spot = pd.read_sql(sql, self.com_conn)
        return spot

    def get_sf(self):
        tbList = self.sf_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'order by name;").fetchall()
        for i in range(len(tbList)):
            tbList[i] = tbList[i][0]
        tbName = tbList[-1]  # last table of sf
        sql = "SELECT * from {0};".format(tbName)
        df_content = self.sf_cursor.execute(sql).fetchall()
        df_content = pd.DataFrame(df_content)        # get the content of sf table but format not good
        return df_content

    # concat suggestion price percentage and sf etc
    def refromat_res(self, res, df_content):
        df_content.index = df_content.iloc[:, 1].tolist()   #reindex sf data with spot name
        df_content = df_content.drop([0, 1, 2, 5, 6, 7, 8], axis=1)         #drop the columns not useful
        res = pd.concat([res, df_content], join='outer', axis=1)
        res.columns = ['现货变化', '代码', '期货价格', '180极限', '基差*极限']
        res = res[res.iloc[:, 0] != 0]                   #filtrate the result only fullfil the terms
        return res

    def judge_spot(self, spot, day):
        spot = spot.tail(day)  # 33 days dataframe since today
        columns = [column for column in spot][1::]  # name of column in df, excluding "日期"
        res = {}
        for colName in columns:  # loop for each spot
            singleSpot = spot.loc[:, colName]
            dayMinus = list(singleSpot.iloc[0:day-1]) #0 to 32 days (day -1)
            dayMinusMin = min(dayMinus)   # minimum in 32 days
            dayMinusMax = max(dayMinus)   # maximum in 32 days
            # if spot 0:32 minimum is No.32, and 33 more than 32 then the lowest and return 1
            if singleSpot.iloc[day-2] <= dayMinusMin and singleSpot.iloc[day-1] > singleSpot.iloc[day-2]:
                #percent of increasing of spot
                res[colName] = (singleSpot.iloc[day-1]-singleSpot.iloc[day-2])/singleSpot.iloc[day-2]*100
            # if spot 0:32 maximum is No.32, and 33 less than 32 then the highest and return -1
            elif singleSpot.iloc[day-2] >= dayMinusMax and singleSpot.iloc[day-1] < singleSpot.iloc[day-2]:
                # percent of increasing of spot
                res[colName] = (singleSpot.iloc[day-1]-singleSpot.iloc[day-2])/singleSpot.iloc[day-2]*100
            else:
                res[colName] = 0
        res = pd.DataFrame.from_dict(res, orient='index')  # turn to dataframe
        return res

    def storage_to_excel(self, res):
        res.to_excel(self.excel_path)


if __name__ == '__main__':
    a = ThirtyThree()