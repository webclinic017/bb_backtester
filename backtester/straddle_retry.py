import datetime
import gzip
import json
import multiprocessing
import pickle
import sqlite3
import sys

import backtrader as bt
import pandas as pd
import quantstats as qs
from utils.log import logger_instance
from utils.misc import get_nearest_expiry

logging = logger_instance


# Strategy Details:


class TestStrategy(bt.Strategy):
    params = (("config", ""),)

    def log(self, txt, dt=None):
        """Logging function for this strategy"""
        dt = dt or self.datas[0].datetime.datetime(0)
        logging.info("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        self.order_close = None
        self.order1_close = None
        self.order = None
        self.order1 = None
        self.sl = 0.3
        self.ce_retry_counter = 0
        self.pe_retry_counter = 0
        self.ce_price = None
        self.pe_price = None
        self.skip_all_remaining_candles = False
        self.only_pe_position = False
        self.only_ce_position = False
        self.config = self.params.config
        self.capital = self.config["capital"]
        self.tsl_delta = self.config["trailing_sl_delta_increment"]
        self.reset_pnl_variables()

    def reset_pnl_variables(self):
        self.pnl = 0
        self.ce_pnl = 0
        self.pe_pnl = 0
        self.tsl_activated = False
        self.tsl_start = self.config["trailing_sl_percentage"]
        self.tsl_start_original = self.config["trailing_sl_percentage"]
        self.strategy_profit = self.config["capital"] * self.tsl_start

    def notify_trade(self, trade):
        # Closed positions PNL
        # self.log("Trade executed: {}".format(trade))
        # if trade data is CE then add to CE PNL else add to PE PNL
        if trade.data._name == "CE":
            self.ce_pnl += trade.pnl
        else:
            self.pe_pnl += trade.pnl
        self.pnl += trade.pnl

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    "BUY EXECUTED {} for {}".format(
                        order.executed.price, order.product_type
                    )
                )
            elif order.issell():
                self.log(
                    "SELL EXECUTED {} for {}".format(
                        order.executed.price, order.product_type
                    )
                )

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected for {}".format(order.product_type))

    def next(self):
        self.only_pe_position = False
        self.only_ce_position = False
        data0_ticker = "PE"
        data1_ticker = "CE"
        current_candle_datetime_underlying = self.data2.datetime.datetime()
        dayofweek = self.data0.datetime.datetime().date().strftime("%A")
        # self.log(
        #     "Current candle time: {} ".format(current_candle_datetime_underlying))
        try:
            data0_ticker = self.data0._dataname.loc[
                current_candle_datetime_underlying
            ].ticker
            data1_ticker = self.data1._dataname.loc[
                current_candle_datetime_underlying
            ].ticker
        except Exception as e:
            # import ipdb;ipdb.set_trace()
            # logging.exception(e)
            self.log(
                "Skipping candle as data not available for {} and {} for time {}".format(
                    data0_ticker, data1_ticker, current_candle_datetime_underlying
                )
            )
            return

        if self.config["fixed_sl_percentage"]:
            sl_pe = sl_ce = self.config["fixed_sl_percentage"]

        if self.config["daywise_sl_percentage"]:
            dayofweek = self.data0.datetime.datetime().date().strftime("%A")
            sl_pe = sl_ce = self.config["daywise_sl_percentage"][dayofweek]

        if self.getposition(self.data0) and not self.getposition(self.data1):
            self.only_pe_position = True

        if self.getposition(self.data1) and not self.getposition(self.data0):
            self.only_ce_position = True

        # To enable below condition we need good comparison of returns with and without this condition
        if self.order:
            self.pe_price = self.order.executed.price

        if self.order1:
            self.ce_price = self.order1.executed.price

        if (
            self.data0.datetime.datetime().time().hour
            == self.config["position_initiate_time"]["hour"]
            and self.data0.datetime.datetime().time().minute
            == self.config["position_initiate_time"]["minute"]
        ):
            self.skip_all_remaining_candles = False
            self.tsl_start = self.config["trailing_sl_percentage"]

        if self.skip_all_remaining_candles:
            self.reset_pnl_variables()
            return

        # end_of_day_minute = self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute in [
        #     20, 21]
        end_of_day_minute = (
            True
            if self.data0.datetime.datetime().time() > datetime.time(15, 23, 00)
            else False
        )

        # Calculating open positions PNL
        pos = self.getposition(self.data0)
        comminfo = self.broker.getcommissioninfo(self.data0)
        pe_pnl = comminfo.profitandloss(pos.size, pos.price, self.data0.close[0])

        pos1 = self.getposition(self.data1)
        comminfo1 = self.broker.getcommissioninfo(self.data1)
        ce_pnl = comminfo1.profitandloss(pos1.size, pos1.price, self.data1.close[0])

        total_pnl = pe_pnl + ce_pnl
        # self.log('position pnl: {} trade pnl: {}, total pnl: {}'.format(total_pnl, self.pnl, total_pnl + self.pnl))

        single_mtm_sl = self.config["mtm_sl_percentage"]
        daywise_mtm_sl = self.config["daywise_mtm_sl_percentage"][dayofweek]
        mtm_sl = single_mtm_sl if single_mtm_sl else daywise_mtm_sl
        # Below is exit condition based on portfolio loss
        if not end_of_day_minute and (
            (total_pnl + self.pnl) < (self.config["capital"] * -mtm_sl)
        ):
            if self.getposition(self.data0):
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                self.log(
                    "BUY CREATE MTM SL, {} {}".format(self.data0.close[0], data0_ticker)
                )
                self.pe_retry_counter = 0
                self.pe_price = None

            if self.getposition(self.data1):
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                self.log(
                    "BUY CREATE MTM SL, {} {}".format(self.data1.close[0], data1_ticker)
                )
                self.ce_retry_counter = 0
                self.ce_price = None

            self.log("PNL MTM: {}".format(total_pnl + self.pnl))
            self.skip_all_remaining_candles = True
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None
            self.ce_price = None
            self.pe_price = None
            self.reset_pnl_variables()

        # Trailing Profit MTM Logic
        if self.only_ce_position:
            if self.tsl_start and (ce_pnl + self.ce_pnl) > self.strategy_profit:
                self.lower_strategy_sl = self.tsl_start - self.tsl_delta
                self.upper_strategy_sl = self.tsl_start + self.tsl_delta
                if (ce_pnl + self.ce_pnl) > (self.capital * self.upper_strategy_sl):
                    self.tsl_activated = True
                    self.log(
                        "TSL activated at {} % for profit {}".format(
                            self.upper_strategy_sl, ce_pnl + self.ce_pnl
                        )
                    )
                    self.tsl_start = self.upper_strategy_sl

            if self.tsl_activated and (ce_pnl + self.ce_pnl) < (
                self.capital * self.lower_strategy_sl
            ):
                self.log(
                    "Exiting CE leg only, TSL hit at {} % for profit {}".format(
                        self.lower_strategy_sl, ce_pnl + self.ce_pnl
                    )
                )
                self.log(
                    "BUY CREATE TSL, {} - {}".format(self.data1.close[0], data1_ticker)
                )
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                self.ce_retry_counter = 5
                self.reset_pnl_variables()

        if self.only_pe_position:
            if self.tsl_start and (pe_pnl + self.pe_pnl) > self.strategy_profit:
                self.lower_strategy_sl = self.tsl_start - self.tsl_delta
                self.upper_strategy_sl = self.tsl_start + self.tsl_delta
                if (pe_pnl + self.pe_pnl) > (self.capital * self.upper_strategy_sl):
                    self.tsl_activated = True
                    self.log(
                        "TSL activated at {} % for profit {}".format(
                            self.upper_strategy_sl, pe_pnl + self.pe_pnl
                        )
                    )
                    self.tsl_start = self.upper_strategy_sl

            if self.tsl_activated and (pe_pnl + self.pe_pnl) < (
                self.capital * self.lower_strategy_sl
            ):
                self.log(
                    "Exiting PE leg only, TSL hit at {} % for profit {}".format(
                        self.lower_strategy_sl, pe_pnl + self.pe_pnl
                    )
                )
                self.log(
                    "BUY CREATE TSL, {} - {}".format(self.data0.close[0], data0_ticker)
                )
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                self.pe_retry_counter = 5
                self.reset_pnl_variables()

        retry_counter = self.config["daywise_retry_count"].get(dayofweek, 2)
        # Check if we are in the market
        if not self.getposition(self.data0):
            if (
                self.pe_price
                and self.data0.close[0] < self.pe_price
                and self.pe_retry_counter <= retry_counter
            ):
                self.log(
                    "SELL CREATE, {} for {} on {}, retry #: {}".format(
                        self.data0.close[0],
                        data0_ticker,
                        self.data0.datetime.date().strftime("%A"),
                        self.pe_retry_counter,
                    )
                )
                self.order = self.sell(self.data0)
                self.order.product_type = self.data0._name

            if (
                self.data0.datetime.datetime().time().hour
                == self.config["position_initiate_time"]["hour"]
                and self.data0.datetime.datetime().time().minute
                == self.config["position_initiate_time"]["minute"]
            ):
                self.pe_retry_counter = 0
                self.pe_price = self.data0.close[0] - self.config[
                    "daywise_delta_points_entry"
                ].get(dayofweek, 0)

        else:
            # added end of day condition here to avoid SL trigger and end of day trigger on same tick
            if (
                not end_of_day_minute
                and self.order
                and self.data0.close[0] > (self.order.executed.price * (1 + sl_pe))
                and self.getposition(self.data0)
            ):
                self.log(
                    "BUY CREATE SL, {}, {}-{} - {}".format(
                        self.data0.close[0],
                        self.order.executed.price,
                        self.order.executed.price * (1 + sl_pe),
                        data0_ticker,
                    )
                )
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                self.pe_retry_counter += 1

        if not self.getposition(self.data1):
            if (
                self.ce_price
                and self.data1.close[0] < self.ce_price
                and self.ce_retry_counter <= retry_counter
            ):
                self.log(
                    "SELL CREATE, {} for {} on {}, retry #: {}".format(
                        self.data1.close[0],
                        data1_ticker,
                        self.data1.datetime.date().strftime("%A"),
                        self.ce_retry_counter,
                    )
                )
                self.order1 = self.sell(self.data1)
                self.order1.product_type = self.data1._name

            if (
                self.data1.datetime.datetime().time().hour
                == self.config["position_initiate_time"]["hour"]
                and self.data1.datetime.datetime().time().minute
                == self.config["position_initiate_time"]["minute"]
            ):
                self.ce_price = self.data1.close[0] - self.config[
                    "daywise_delta_points_entry"
                ].get(dayofweek, 0)
                self.ce_retry_counter = 0

        else:
            if (
                not end_of_day_minute
                and self.order1
                and self.data1.close[0] > (self.order1.executed.price * (1 + sl_ce))
                and self.getposition(self.data1)
            ):
                self.log(
                    "BUY CREATE SL, {}, {}-{} - {}".format(
                        self.data1.close[0],
                        self.order1.executed.price,
                        self.order1.executed.price * (1 + sl_ce),
                        data1_ticker,
                    )
                )
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                self.ce_retry_counter += 1

        if end_of_day_minute:
            self.log("PNL EOD: {}".format(total_pnl + self.pnl))
            if self.getposition(self.data0):
                o = self.buy(self.data0)
                o.product_type = self.data0._name
                self.log(
                    "BUY CREATE EOD, {} {}".format(self.data0.close[0], data0_ticker)
                )
                self.pe_retry_counter = 0
                self.pe_price = None

            if self.getposition(self.data1):
                o1 = self.buy(self.data1)
                o1.product_type = self.data1._name
                self.log(
                    "BUY CREATE EOD, {} {}".format(self.data1.close[0], data1_ticker)
                )
                self.ce_retry_counter = 0
                self.ce_price = None
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None
            self.ce_price = None
            self.pe_price = None
            self.skip_all_remaining_candles = True
            self.reset_pnl_variables()

        # # The below because not able to set self.pnl = 0 in above block
        # if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute > 20:
        #     self.pnl = 0


def run_backtest(df_final_pe, df_final_ce, df_final_underlying, config, result_queue):
    cerebro = bt.Cerebro()
    data_pe = bt.feeds.PandasData(dataname=df_final_pe)
    data_ce = bt.feeds.PandasData(dataname=df_final_ce)
    data_underlying = bt.feeds.PandasData(dataname=df_final_underlying)
    cerebro.adddata(data_pe, name="PE")
    cerebro.adddata(data_ce, name="CE")
    cerebro.adddata(data_underlying, name="underlying")

    print("Run start")
    logging.info("Logging Config {}".format(config))
    cerebro.addstrategy(TestStrategy, config=config)
    cerebro.addsizer(bt.sizers.SizerFix, stake=config["quantity"])
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name="pyfolio")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="tanal")
    cerebro.broker = bt.brokers.BackBroker(slip_perc=0.005)
    cerebro.broker.setcash(config["capital"])
    strats = cerebro.run()
    pyfolio = strats[0].analyzers.getbyname("pyfolio")
    returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
    result_queue.put(returns)


def serialise_save_file(df, filename):
    with gzip.open(filename, "wb") as f:
        logging.info("Serializing input to file {}".format(filename))
        pickle.dump(df, f)


def deserialize_save_file(filename):
    with gzip.open(filename, "rb") as f:
        logging.info("Reading serialized input from file".format(filename))
        df = pickle.load(f)
    return df


def serialize_data(df_input_pe, df_input_ce, df_input_underlying, config):
    if not config["read_from_serialized_input"]:
        serialise_save_file(df_input_pe, config["serialize_input_filename_pe"])
        serialise_save_file(df_input_ce, config["serialize_input_filename_ce"])
        serialise_save_file(
            df_input_underlying, config["serialize_input_filename_underlying"]
        )


def deserialize_data(config):
    df_input_pe = deserialize_save_file(config["serialize_input_filename_pe"])
    df_input_ce = deserialize_save_file(config["serialize_input_filename_ce"])
    df_input_underlying = deserialize_save_file(
        config["serialize_input_filename_underlying"]
    )
    return df_input_pe, df_input_ce, df_input_underlying


def split_df_for_multiprocessing(df):
    # Step 1: Identify Unique Days
    # df['date_only'] = df['date'].dt.date  # Extract the date from datetime

    split_dfs_dict = {}

    df["date_only"] = df.index.date
    unique_days = df["date_only"].unique()

    # Step 2: Group Days into 'N' Bins
    N = 4  # Example value for 'N'
    days_per_group = len(unique_days) // N
    grouped_days = [
        unique_days[i : i + days_per_group]
        for i in range(0, len(unique_days), days_per_group)
    ]

    # Step 3: Split the DataFrame and store in the split_dfs_dict based on N and drop date_only column
    for i in range(N):
        split_dfs_dict[i] = df[df["date_only"].isin(grouped_days[i])]
        split_dfs_dict[i] = split_dfs_dict[i].drop(columns=["date_only"])

    # Step 3: Split the DataFrame
    # split_dfs = [df[df['date_only'].isin(group)] for group in grouped_days]

    return split_dfs_dict


def main():
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(sys.argv[0], "<filename.json>"))
        exit(1)

    file_name = sys.argv[1]
    with open(file_name) as config_file:
        config = json.load(config_file)

    strike_dbpath = config["strike_dbpath"]
    underlying_dbpath = config["underlying_dbpath"]
    start_date = config["start_date"]
    end_date = config["end_date"]
    table_name = config["strike_table_name"]
    underlying_table_name = config["underlying_table_name"]
    strangle_delta = config["strangle_delta"]

    con = sqlite3.connect(strike_dbpath)
    con1 = sqlite3.connect(underlying_dbpath)
    df_final_pe = pd.DataFrame()
    df_final_ce = pd.DataFrame()
    df_final_underlying = pd.DataFrame()
    datelist = pd.date_range(start=start_date, end=end_date).tolist()
    start_date = str(datelist[0].date())
    end_date = str(datelist[-1].date())

    if config["read_from_serialized_input"]:
        datelist = []

    for d in datelist:
        d = d.date()
        d1 = get_nearest_expiry(d)
        ohlc = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "oi": "sum",
        }
        underlying_query_string = "SELECT * from {} where date(date) = date(?)".format(
            underlying_table_name
        )
        df_fut = pd.read_sql_query(
            underlying_query_string,
            con1,
            params=[d],
            parse_dates=True,
            index_col="date",
        )
        if df_fut.empty:
            logging.info("Skipping : {}".format(d))
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        # for example if we are taking a position at 09:20 then [4] ,09:30 then [15] as these are 1 minute candles
        close = df_fut.iloc[15].close
        atm_strike = round(close / 50) * 50  # round to nearest 50
        ce_strike = atm_strike + strangle_delta
        pe_strike = atm_strike - strangle_delta
        logging.info(
            "Trade date : {} - Picking ce strike - {} , pe strike - {}, atm strike - {}  for expiry {}, underlying: {}".format(
                d, ce_strike, pe_strike, atm_strike, d1, close
            )
        )
        query_string = "SELECT distinct * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
            table_name
        )
        df_opt_pe = pd.read_sql_query(
            query_string,
            con,
            params=[pe_strike, d, d1, "PE"],
            parse_dates=True,
            index_col="date",
        )
        df_opt_ce = pd.read_sql_query(
            query_string,
            con,
            params=[ce_strike, d, d1, "CE"],
            parse_dates=True,
            index_col="date",
        )
        df_opt_pe.index = pd.to_datetime(df_opt_pe.index)
        df_opt_pe = df_opt_pe.sort_index()

        df_opt_ce.index = pd.to_datetime(df_opt_ce.index)
        df_opt_ce = df_opt_ce.sort_index()
        # skip the day if first tick is not at 9:15
        # if not df_opt_ce.empty and (df_opt_ce.index[0].minute != 15 or df_opt_pe.index[0].minute != 15):
        #     continue
        # skip the day if the number of elements in either df_opt_ce or df_opt_pe is less than 375 count.
        if not df_opt_ce.empty and (len(df_opt_ce) < 370 or len(df_opt_pe) < 370):
            logging.info(
                "Skipping : {} as data is incomplete , count : {}".format(
                    d, len(df_opt_ce)
                )
            )
            continue

        df_final_ce = df_final_ce.append(df_opt_ce)
        df_final_pe = df_final_pe.append(df_opt_pe)
        df_final_underlying = df_final_underlying.append(df_fut)

    serialize_data(df_final_pe, df_final_ce, df_final_underlying, config)

    if config["read_from_serialized_input"]:
        df_final_pe, df_final_ce, df_final_underlying = deserialize_data(config)

    df_final_ce["day_of_week"] = df_final_ce.index.day_name()
    df_final_pe["day_of_week"] = df_final_pe.index.day_name()
    df_final_underlying["day_of_week"] = df_final_underlying.index.day_name()

    # Filter out specific days ex: "Tuesday"
    blind_skip_days_list = config["blind_skip_days_list"]
    df_final_ce = df_final_ce[~df_final_ce["day_of_week"].isin(blind_skip_days_list)]
    df_final_pe = df_final_pe[~df_final_pe["day_of_week"].isin(blind_skip_days_list)]
    df_final_underlying = df_final_underlying[
        ~df_final_underlying["day_of_week"].isin(blind_skip_days_list)
    ]

    dfs_by_n_ce = split_df_for_multiprocessing(df_final_ce)
    dfs_by_n_pe = split_df_for_multiprocessing(df_final_pe)
    dfs_by_n_underlying = split_df_for_multiprocessing(df_final_underlying)

    result_queue = multiprocessing.Queue()
    processes = []

    for i in dfs_by_n_underlying.keys():
        p = multiprocessing.Process(
            target=run_backtest,
            args=(
                dfs_by_n_pe.get(i),
                dfs_by_n_ce.get(i),
                dfs_by_n_underlying.get(i),
                config,
                result_queue,
            ),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Retrieve results from the queue and sort them by date
    returns = []
    while not result_queue.empty():
        returns.append(result_queue.get())

    # import ipdb;ipdb.set_trace()
    returns = pd.concat(returns, axis=0)
    # sort the returns by date
    returns = returns.sort_index()

    returns.index = returns.index.tz_convert(None)
    qs.extend_pandas()
    qs.reports.html(
        returns,
        output=config["output_filename"],
        download_filename=config["output_filename"],
        title=config["strategy_name"],
    )
    qs.reports.metrics(returns, mode="full", display=True)

    monthly_returns = returns.monthly_returns() * 100
    df = returns.to_frame(name="daily_returns")
    df["weekday"] = df.index.day_name()

    weekday_returns = df.groupby("weekday")["daily_returns"].mean() * 100

    # Print the metrics
    print("\nMonthly Returns:")
    print(monthly_returns)

    print("\nWeekday-wise Avg. Returns:")
    for index, value in weekday_returns.iteritems():
        print(f"{index}: {value:.4f}")

    df["negative_returns"] = df["daily_returns"] <= 0
    # print('Final Portfolio Value: ${}'.format(portvalue))
    print("Run finish")


if __name__ == "__main__":
    main()
