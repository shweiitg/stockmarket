import pandas as pd
from datetime import datetime, date
import yfinance as yf

column = ['Name', 'mon_open', 'mon_close', 'tue_open', 'tue_close', 'wed_open', 'wed_close',
              'tur_open', 'tur_close', 'fri_open', 'fri_close']
def downloadStockPrice(stock, stocks_csv, failed, startDate, endDaete):
    st = startDate.strftime('%Y-%m-%d')
    end = endDaete.strftime('%Y-%m-%d')
    #end = endDate.strftime('%Y-%m-%d')
    stock_data = yf.download(stock + '.BO',
                     start=st,
                     end=end,
                     progress=False)
    #print(stock_data)
    return stock_data
    """
    voln = df['Volume'].size
    if (voln <= 0):
        failed.append(stock)
        print('For {} zero volume'.format(stock))
    else:
        df = df.iloc[::-1]
        index = stock.find('.')
        if index > 0:
            df['STOCK'] = stock[:index]
        else:
            df['STOCK'] = stock
        df.to_csv(stocks_csv, mode='a', header=False)
    """

def createRow(name, rows):


    #stock_weekly_report = pd.DataFrame(columns=column)
    #stock_weekly_report['Name'] = name
    mon_open, mon_close, tue_open, tur_close, wed_open, wed_close, tur_open, tue_close, fri_open, fri_close = \
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    for index, row in rows.iterrows():
        #print(row)
        if index.dayofweek == 0:
            mon_open = row.Open
            mon_close = row.Close
        elif index.dayofweek == 1:
            tue_open = row.Open
            tue_close = row.Close
        elif index.dayofweek == 2:
            wed_open = row.Open
            wed_close = row.Close
        elif index.dayofweek == 3:
            tur_open = row.Open
            tur_close = row.Close
        elif index.dayofweek == 4:
            fri_open = row.Open
            fri_close = row.Close

    stock_weekly_report = pd.Series([name, mon_open, mon_close, tue_open, tur_close, wed_open, wed_close,
                                      tur_open, tue_close, fri_open, fri_close],
                        index=column)
    return stock_weekly_report


df = pd.read_csv("downloads//Equity.csv")
print(df['Security Id'])
start = date(2021, 6, 7)
end = date(2021, 6, 12)
stocks_csv = None
failedList = []
stock_weekly_report = None
stock_weekly_report_list = pd.DataFrame(columns=column)
for index, row in df.iterrows():
    securityId = row['Security Id']
    stock_data = downloadStockPrice(securityId, stocks_csv, failedList, start, end)
    if stock_data.empty:
        failedList.append(securityId)
    else:
        stock_weekly_report = createRow(securityId, stock_data)
        stock_weekly_report_list = stock_weekly_report_list.append(stock_weekly_report, ignore_index=True)

print(failedList)

stock_weekly_report_list.to_csv('../database/result.csv', index=False)
with open('../database/failed_download.txt', 'w') as f:
    for item in failedList:
        f.write("%s\n" % item)







