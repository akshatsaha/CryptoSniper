from __future__ import annotations
from os import sep, path, pardir, system
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from time import sleep
from subprocess import run as Run, PIPE as Pipe
from re import findall as FindAll
import subprocess
import sys
import uuid

# Custom stdout redirection class to capture print statements
class LoggerWriter:
    """Custom stream handler that redirects stdout/stderr to a logger
    
    This class creates a file-like object that can replace sys.stdout
    and sys.stderr, redirecting all print statements to a logger.
    """
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.buffer = ''
        
    def write(self, message):
        # If message ends with a newline, process the complete line
        if message.endswith('\n'):
            self.buffer += message.rstrip('\n')
            self.flush()
        else:
            # Otherwise, accumulate in buffer
            self.buffer += message
            
    def flush(self):
        if self.buffer:
            self.logger.log(self.level, self.buffer)
            self.buffer = ''
            
    def isatty(self):
        # This method is needed for some libraries that check if stdout is a tty
        return False


def setup_logger(name, log_level=logging.INFO, log_to_console=False, log_to_file=True, log_file=None, max_file_size=100*1024*1024, backup_count=3, capture_print=False, capture_other_loggers=False):
    # Set default log file path
    if log_file is None:
        log_file = os.path.join("/home/ubuntu/logs", f"{name}.log")
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.handlers = []

    if not capture_other_loggers:
        logger.propagate = False
    
    # Create formatter
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s')
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if log_to_file:
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except PermissionError:
                logger.error(f"Permission denied when creating log directory: {log_dir}")
                return logger
            
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    if capture_print:
        # Save original stdout/stderr
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        # Create and set custom stdout/stderr handlers
        sys.stdout = LoggerWriter(logger, logging.INFO)
        sys.stderr = LoggerWriter(logger, logging.ERROR)
        
        # Store originals in logger for potential restoration
        logger.original_stdout = original_stdout
        logger.original_stderr = original_stderr
        logger.info(f"Stdout/stderr redirected to logger '{name}'")
    
    return logger

def set_log_level(logger, level):
    """Dynamically change the log level of a logger
    
    Args:
        logger (logging.Logger): The logger to modify
        level: The new log level (e.g., logging.DEBUG, logging.INFO)
    """
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)
    logger.info(f"Log level changed to {logging.getLevelName(level)}")
    
    return logger

def restore_stdout_stderr(logger):
    """Restore original stdout/stderr if they were redirected
    
    Args:
        logger (logging.Logger): The logger that has redirected stdout/stderr
    """
    if hasattr(logger, 'original_stdout') and hasattr(logger, 'original_stderr'):
        sys.stdout = logger.original_stdout
        sys.stderr = logger.original_stderr
        logger.info("Restored original stdout/stderr")
    else:
        logger.warning("No redirected stdout/stderr found to restore")
    
    return logger

def create_kline_channel(symbol, interval="1m"):
    """
    Create a WebSocket channel for kline data
    
    Args:
        symbol (str): Trading pair symbol (e.g., "ETH-USDT")
        interval (str): Kline interval (e.g., "1m", "5m", "1h")
        
    Returns:
        dict: Channel configuration
    """
    return {
        "id": str(uuid.uuid4()),
        "reqType": "sub",
        "dataType": f"{symbol}@kline_{interval}"
    }

def create_trade_channel(symbol):
    """
    Create a WebSocket channel for trade data
    
    Args:
        symbol (str): Trading pair symbol (e.g., "ETH-USDT")
        
    Returns:
        dict: Channel configuration
    """
    return {
        "id": str(uuid.uuid4()),
        "reqType": "sub",
        "dataType": f"{symbol}@trade"
    }

def create_depth_channel(symbol, depth_level=5, update_frequency="500ms"):
    """
    Create a WebSocket channel for market depth data
    
    Args:
        symbol (str): Trading pair symbol (e.g., "ETH-USDT")
        depth_level (int): Depth level (5, 10, 20, 50, 100)
        update_frequency (str): Update frequency (e.g., "500ms")
        
    Returns:
        dict: Channel configuration
    """

    return {
        "id": str(uuid.uuid4()),  # Generate a random UUID
        "reqType": "sub",
        "dataType": f"{symbol}@depth{depth_level}@{update_frequency}"
    }

### code runner related code

def RunScreens():

    screen = Run(["screen", "-ls"], stdout=Pipe, text=True).stdout
    screens = FindAll(r"([0-9]+)\.(\w+)", screen)

    return screens

def kill_me(screen_name):

    screen = Run(["screen", "-ls"], stdout=Pipe, text=True).stdout
    pids = FindAll(rf"([0-9]+)\.{screen_name}\t", screen)

    for pid in pids:
        print(pid, screen_name, 'Kill')
        subprocess.call(f"screen -XS {pid} kill", shell=True)

def kill_all_screens():

    try:
        _ = subprocess.run(
            "screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs kill", shell=True
        )

        return None

    except Exception as e:
        print(e)

def file_path_locator():

    wd = path.abspath(path.dirname(__file__))
    folder = wd + sep + pardir  # pardir Goes Back 1 Folder

    return folder

def file_name(filename, folder, ftype):

    if ftype == "logs":

        dirname = path.join(path.normpath(folder), "screenlogs")

        fname = path.join(dirname, f"{filename}.log")

        return fname

    elif ftype == "code":

        dirname = path.join(path.normpath(folder), "code")

        fname = path.join(dirname, f"{filename}.py")

        return fname

    elif ftype == "Dashboard":

        dirname = path.join(path.normpath(folder), "code/Dashboard")

        fname = path.join(dirname, f"{filename}.py")

        return fname

def code_runner(ID, CodePath, LogPath, Params, logger) -> None:
    """Runs the Code With the Given Parameters"""
    log_dir = "/home/ubuntu/logs"

    # Use the correct Python interpreter path /home/ubuntu/venv/bin/python3
    Command = f"screen -dmSL {ID} -Logfile {LogPath} /home/ubuntu/venv/bin/python3 {CodePath}"
    print(Command)

    for Param in Params:
        Command += f" {Param}"

    logger.info(f"{ID} \n  {Command}")

    _ = subprocess.call(Command, shell=True)

    return None

def file_run(filename, logger, logname=None, params=[]):

    logname = logname if logname else filename
    folder = file_path_locator()

    codepath = file_name(filename=filename, folder=folder, ftype="code")
    logpath = file_name(filename=logname, folder=folder, ftype="logs")

    # print(folder)
    # print(codepath)
    # print(logpath)

    kill_me(logname)

    code_runner(ID=logname, CodePath=codepath, LogPath=logpath, Params=params, logger=logger)


