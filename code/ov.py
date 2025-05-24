from Utils import *
import argparse


if __name__ == "__main__":

    strategy = {
        "1000":"ETH_Multiplier",
        "2000":"Bit Bounce",
        "3000":"Bullish Sniper"
    }
    
    current_file = str(os.path.basename(__file__)).replace(".py","")
    LOG_FILE = f"/home/ubuntu/CRYPTOCODE/logs/{current_file}.log"
    logger = setup_logger(name=current_file,log_to_file=True,log_file=LOG_FILE)

    # file_run("LiveCandle",logger)
    # file_run("backend", logger, logname="backend_server")

    # file_run("FRC_SPT_STRA",logger,logname="SOL_Multiplier", params=["SOL_Multiplier"])
    # file_run("new_order",logger,logname="SOL_Multiplier_order",params=["SOL_Multiplier"])

    file_run("FRC_SPT_STRA",logger,logname="ETH_Multiplier", params=["ETH_Multiplier"])
    # file_run("new_order",logger,logname="ETH_Multiplier_order",params=["ETH_Multiplier"])

    # file_run("FRC_SPT_STRA",logger,logname="BTC_Multiplier", params=["BTC_Multiplier"])
    # file_run("new_order",logger,logname="BTC_Multiplier_order",params=["BTC_Multiplier"])

    ####################################################################################################

    # file_run("EMA_CROSS_STRA",logger,logname="Bit_Bounce", params=["Bit_Bounce"])
    # file_run("new_order",logger,logname="Bit_Bounce_order",params=["Bit_Bounce"])

    # file_run("EMA_CROSS_STRA",logger,logname="ETH_Multiplier", params=["ETH_Multiplier"])
    # file_run("new_order",logger,logname="ETH_Multiplier_order",params=["ETH_Multiplier"])

    # file_run("EMA_CROSS_STRA",logger,logname="BTC_Multiplier", params=["BTC_Multiplier"])
    # file_run("new_order",logger,logname="BTC_Multiplier_order",params=["BTC_Multiplier"])

    kill_me("Bit_Bounce")
    kill_me("Bit_Bounce_order")
    # kill_me("ETH_Multiplier")
    # kill_me("ETH_Multiplier_order")
    # kill_me("BTC_Multiplier")
    # kill_me("BTC_Multiplier_order")


    pass
