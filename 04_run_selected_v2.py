import pandas as pd
import datetime

import os, time
from multiprocessing import Process
df = pdf.

def append_rows(stocks, dp, volatility, df, stock_buy, stock_check):
    if (volatility >= 0):
        subset_df = stocks[stocks[dp] >= volatility]
        save_rows(subset_df, dp, df, volatility, stocks, stock_buy, stock_check)
    else:
        subset_df = stocks[stocks[dp] <= volatility]
        save_rows(subset_df, dp, df, volatility, stocks, stock_buy, stock_check)

def run_instance(dp, stocks, stock_buy, stock_check):
    for volatility in get_volatility(stocks, dp):
        index = 0
        column_df = [k for k in stocks.columns if 'df' in k]
        for df in column_df:
            append_rows(stocks, dp, volatility, df, stock_buy, stock_check)

def save_rows(subset_df, dp, df, volatility, stocks, stock_buy, stock_check):
    n = len(subset_df)
    rows_df = pd.DataFrame(
        {'Date': subset_df["Date"],
        'DP': [dp]*n,
        'DF': [df]*n,
        'Cutoff':[volatility]*n,
        'Occurences':[len(subset_df)]*n,
        'sb_change':subset_df[df],
        'Average_change':[stocks[df].mean()]*n})
    rows_df.to_csv('{}_{}_analysis.csv'.format(stock_buy,stock_check), mode='a', header=False)


def organize_stocks(stock_buy, stock_check):
    stocks = read_stocks(stock_buy, stock_check)
    stocks = change_previous(stocks)
    stocks = change_future(stocks)
    return stocks

def get_volatility(stock_df, days_previous):
    cutoffs = []
    occurences = [50, 75, 100, 150, 200, 250, 300, 400, 500]
    for occurence in occurences:
        try:
            x = stock_df.nlargest(occurence, days_previous)
            index = occurence - 1
            cutoffs.append(x[days_previous].iloc[index])
        except:
            pass
    for occurence in occurences:
        try:
            x = stock_df.nsmallest(occurence, days_previous)
            index = occurence - 1
            cutoffs.append(x[days_previous].iloc[index])
        except:
            pass
    return cutoffs

def create_blank_df(stock_buy, stock_check):
    results = get_blank_df()
    results.to_csv('{}_{}_analysis.csv'.format(stock_buy,stock_check))

def get_blank_df():
    df_stock_columns = pd.DataFrame(
        columns=["Date", 'DP', 'DF', 'Cutoff', 'Occurrences', 'sb_Change', 'Average_change'])
    return df_stock_columns

def change_previous(stock_data):
    try:
        for day in range(1, 50):
            days_previous = day
            column_name = str(days_previous) + "dp"
            stock_data[column_name] = 0
            for index in range(days_previous, len(stock_data)):
                day_before = stock_data['sc_price'].iloc[index - days_previous]
                day_current = stock_data['sc_price'].iloc[index]
                change_top = day_current - day_before
                change_day = change_top / day_before
                stock_data[column_name].iloc[index] = change_day
    except:
        pass
    return stock_data

def change_future(stock_data):
    #for day in days_range:
    for days_future in range(1, 50):
        column_name = str(days_future) + "df"
        stock_data[column_name] = 0
        loop_end = len(stock_data) - days_future
        for index in range(0, loop_end):
            day_current = stock_data['sb_price'].iloc[index]
            day_future = stock_data['sb_price'].iloc[index + days_future]
            change_top = day_future - day_current
            change_day = change_top / day_current
            stock_data[column_name].iloc[index] = change_day
    return stock_data

def read_stocks(stock_buy, stock_check):
    sb = pd.read_csv(
        "stock_data\{}.csv".format(
            stock_buy))
    sc = pd.read_csv(
        "stock_data\{}.csv".format(
            stock_check))
    sb = sb[["Date", "close"]]
    sc = sc[["Date", "close"]]
    combined = pd.merge(sb, sc, on='Date', how="outer")
    combined.columns = ["Date", "sb_price", "sc_price"]
    combined = combined.dropna()
    return combined

def run_full(stock_buy, stock_check):
    stocks = organize_stocks(stock_buy,stock_check)
    create_blank_df(stock_buy, stock_check)
    column_dp = [k for k in stocks.columns if 'dp' in k]
    print('started analysis')
    for dp in column_dp:
        run_instance(dp, stocks, stock_buy, stock_check)

# if __name__ == "__main__":
#     stock_buy = "msft"
#     stock_check = "ABBV"
#     # processes = []
#     stocks = organize_stocks(stock_buy,stock_check)
#     create_blank_df(stock_buy, stock_check)
#     column_dp = [k for k in stocks.columns if 'dp' in k]
#     print('started analysis')
#     for dp in column_dp:
#         run_instance(dp, stocks, stock_buy, stock_check)
#         # try:
        #     p = Process(target=run_instance, args=(
        #                     dp,
        #                     stocks,
        #                     stock_buy,
        #                     stock_check
        #                 ))
        #     processes.append(p)
        # except:
        #     print("Process failed: " + '  dp: '+dp)

    # for process in processes:
    #     try:
    #         process.start()
    #         time.sleep(.4)

    #     except:
    #         print("process failed")
