import pymongo
from dotenv import load_dotenv
from time import sleep as Sleep
from pause import until as Until
from os import sep, path, pardir, getenv, system
from datetime import datetime, timedelta, time as dt_time
from Utils import *


def RunApp():

    folder = file_path_locator()
    codefolder = folder + sep
    codepath = file_name(filename="app", folder=codefolder, ftype="Dashboard")
    logpath = file_name(filename="app", folder=folder, ftype="logs")

    print(codepath)
    print(logpath)

    kill_me("app")

    Command = (
        f"screen -dmSL app -Logfile {logpath} authbind /usr/bin/python3 {codepath}"
    )

    status = subprocess.call(Command, shell=True)

    # print(Command)
    # print(status)

    return None

def ticks_runner(roots):

    for root in roots:
        ws_list = tick_client[root][Tick.SUB].distinct("WS")
        for index in ws_list:
            logname = f"{root}_{index}"
            file_run(
                filename="ticks",
                params=[root, index],
                logger=logger,
                logname=logname,
            )

def DoOver():
    logname = filename = "DoOver"
    folder = file_path_locator()

    codepath = file_name(filename=filename, folder=folder, ftype="code")
    logpath = file_name(filename=filename, folder=folder, ftype="logs")

    Command = f"screen -mSL {filename} -Logfile {logpath} /usr/bin/python3 {codepath}"
    print(Command)
    status = system(Command)

if __name__ == "__main__":

    load_dotenv()
    WS_URL = getenv("WS_URL")
    CHAT_ID = getenv("CHAT_ID")
    API_TOKEN = getenv("API_TOKEN")
    TICKS_URL = getenv("TICKS_URL")
    TELEGRAM_URL = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"

    filename = path.basename(__file__)[:-3]
    folder = file_path_locator()

    logger = log_setup(filename, folder)

    tick_client = pymongo.MongoClient(TICKS_URL)

    # RunApp()

    Until(datetime.now().replace(minute=0, hour=9, microsecond=0, second=0))

    while True:

        check_date = datetime.now()
        check_time = check_date.time().replace(second=0, microsecond=0)

        if 0 <= check_date.weekday() < 5:

            if check_time == dt_time(9, 2):
                pass
                # file_run("trades_deleter", logger)
            
            elif (
                (check_time == dt_time(9, 5) and check_date.weekday() == 4)
                or (check_time == dt_time(9, 5) and check_date.weekday() == 3)
                or (check_time == dt_time(9, 5) and check_date.weekday() == 2)
            ):
                pass
                # file_run("token_downloader", logger)
            
            elif check_time == dt_time(9, 8):
                pass
                # file_run("candles_downloader", logger, params=[1])
            
            elif check_time == dt_time(9, 12):
                pass
                # file_run("access_token", logger)

            elif check_time == dt_time(9, 14):
                pass
                # file_run("ticks_to_candles", logger)

            elif check_time == dt_time(9, 15):
                # ticks_runner(
                #     [Root.BNF,
                #      Root.NF
                #      ]
                # )  # , Root.NF, Root.FNF, Root.MIDCPNF, Root.SENSEX])

                # file_run("spot_ticks", logger)

                # file_run("order_placer", logger, "ratio_02_orders", params=[Strat.R2])

                file_run("ratio_02", logger)

                # file_run("order_placer", logger, "ratio_01_orders", params=[Strat.R1])

                file_run("ratio_01", logger)

                file_run("ratio_01_msgs", logger)

                file_run("ratio_02_msgs", logger)

                # file_run("fetch_data", logger)

                # file_run("HT_one_candle", logger)
            elif check_time == dt_time(9,19):
                file_run("Guppy_live_2", logger)
                file_run("New_Guppy_Live", logger)
                file_run("guppy_msg", logger)
                file_run("test", logger, f"{Strat.GP}_orders", params=[Strat.GP])


            elif check_time == dt_time(9,25):
                # file_run("spt_Orders", logger)
                file_run("spt_strategy", logger)
                file_run("spt_msg", logger)


            elif check_time == dt_time(9,30):
                pass
                # file_run("order_placer", logger, f"{Strat.BR}_orders", params=[Strat.BR])

                # file_run("gap_rev", logger, Strat.BR, params=[Strat.BR])

                # file_run("gap_msgs", logger, Strat.BR + "_msgs", params=[Strat.BR])


                # file_run("order_placer", logger, f"{Strat.NR}_orders", params=[Strat.NR])

                # file_run("gap_rev", logger, Strat.NR, params=[Strat.NR])

                # file_run("gap_msgs", logger, Strat.NR + "_msgs", params=[Strat.NR])


                # file_run("order_placer", logger, f"{Strat.BC}_orders", params=[Strat.BC])

                # file_run("gap_cont", logger, Strat.BC, params=[Strat.BC])

                # file_run("gap_msgs", logger, Strat.BC + "_msgs", params=[Strat.BC])


                # file_run("order_placer", logger, f"{Strat.NC}_orders", params=[Strat.NC])

                # file_run("gap_cont", logger, Strat.NC, params=[Strat.NC])

                # file_run("gap_msgs", logger, Strat.NC + "_msgs", params=[Strat.NC])

            elif check_time == dt_time(14, 56):
                file_run("ratio_01_msgs", logger)
                file_run("ratio_02_msgs", logger)
                # file_run("gap_msgs", logger, Strat.BR + "_msgs", params=[Strat.BR])
                # file_run("gap_msgs", logger, Strat.NR + "_msgs", params=[Strat.NR])
                # file_run("gap_msgs", logger, Strat.BC + "_msgs", params=[Strat.BC])
                # file_run("gap_msgs", logger, Strat.NC + "_msgs", params=[Strat.NC])

            elif check_time == dt_time(15, 36):
                # Killer()
                pass

        if check_time == dt_time(15, 37):
            DoOver()

        print(datetime.now().replace(second=0, microsecond=0) + timedelta(minutes=1))
        Until(datetime.now().replace(second=0, microsecond=0) + timedelta(minutes=1))
