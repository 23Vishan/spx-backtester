import os
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
import cProfile

import numpy as np
import matplotlib.pyplot as plt

exe_path = "F:/Workplace/Compression.exe"

def stop_limit_order(date, lower_strike, upper_strike, timestamp, stop_price, limit_price, is_call):
    global exe_path
    
    lower_strike_table_name = ('C' if is_call else 'P') + str(lower_strike)
    upper_strike_table_name = ('C' if is_call else 'P') + str(upper_strike)
    
    args = [date, lower_strike_table_name, upper_strike_table_name, str(timestamp), str(stop_price), str(limit_price), "stoplimitorder"]
    cmd = [exe_path] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    tokens = result.stdout.split()
    if tokens is not None and len(tokens) >= 2:
        exit_time = int(tokens[0]) if not None else 0
        rounded_value_of_position = round(float(tokens[1]), 2) if not None else 0   
        return(exit_time, rounded_value_of_position)
    else:
        return None

def stop_loss(date, lower_strike, upper_strike, timestamp, entry_credit, stop_multiplier, is_call):
    global exe_path
    
    lower_strike_table_name = ('C' if is_call else 'P') + str(lower_strike)
    upper_strike_table_name = ('C' if is_call else 'P') + str(upper_strike)
    
    args = [date, lower_strike_table_name, upper_strike_table_name, str(timestamp), str(entry_credit), str(stop_multiplier), "stoploss"]    
    cmd = [exe_path] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    tokens = result.stdout.split()
    if tokens is not None and len(tokens) >= 2:
        exit_time = int(tokens[0]) if not None else 0
        rounded_value_of_position = round(float(tokens[1]), 2) if not None else 0
        return(exit_time, rounded_value_of_position)
    else:
        return None

def find_spread_search_range(day):
    path = "processed-data/" + day
    dir_list = os.listdir(path) 

    for index, table_name in enumerate(dir_list):
        next_table_name = dir_list[index + 1]
        strike = int(table_name[1:])
        next_strike = int(next_table_name[1:])

        if next_strike - strike == 5:
            search_lower_bound = strike
            break

    for i in range(len(dir_list) - 1, 0, -1):
        table_name = dir_list[i]
        previous_table_name = dir_list[i-1]
        strike = int(table_name[1:])
        previous_strike = int(previous_table_name[1:])

        if strike - previous_strike == 5:
            search_upper_bound = strike
            return (search_lower_bound, search_upper_bound)

def find_strikes(date, timestamp_of_entry, entry_credit, spread_width, num_spreads):
    global exe_path
    
    search = find_spread_search_range(date)
    search_lower = str(search[0])
    search_upper = str(search[1])
    
    # convert arguments to strings
    args = [str(arg) for arg in [date, timestamp_of_entry, spread_width, entry_credit, num_spreads, search_lower, search_upper]]
    args.append("both")
    cmd = [exe_path] + args

    # run executable
    result = subprocess.run(cmd, capture_output=True, text=True)
    parts = result.stdout.split("break")

    # split data into calls and puts
    calls = parts[0].strip().split('\n')
    puts = parts[1].strip().split('\n')

    # parse output to list
    calls_spreads = [(int(tokens[0]), int(tokens[1]), float(tokens[2])) for tokens in (line.split() for line in calls) if tokens and len(tokens) >= 3]
    puts_spreads = [(int(tokens[0]), int(tokens[1]), float(tokens[2])) for tokens in (line.split() for line in puts) if tokens and len(tokens) >= 3]
    return calls_spreads or [], puts_spreads or []

# MAIN --------------------------------------------------------------------------------------------------------------------------
def process_spreads(timestamp_of_entry, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 3, 1)
    
    # list of dates to backtest
    date_range = [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date - start_date).days + 1)]
    directory = 'processed-data'
    files = os.listdir(directory)
    filtered_dates = [file for file in files if file in date_range]

    total_profit = 0
    for date in filtered_dates:
        
        # get spreads
        call_spreads, put_spreads = find_strikes(date, timestamp_of_entry, entry_credit, spread_width, num_spreads)
        spreads = call_spreads + put_spreads 
        print(date)
        #print(spreads)
        
        # process each spread
        for i, (lower_strike, upper_strike, spread_credit) in enumerate(spreads):
            is_call = i < len(call_spreads)
            
            # attach stop limit order to determine when to enter
            slo = stop_limit_order(date, lower_strike, upper_strike, timestamp_of_entry, stop_price, limit_price, is_call)
            
            slo_et = slo[0] if slo else None # exit time
            slo_ec = slo[1] if slo else None # execution credit
            
            # if entered, attach stop loss
            if slo_et is not None:
                sl = stop_loss(date, lower_strike, upper_strike, slo_et, slo_ec, sl_mult, is_call)
                
                sl_et = sl[0] if sl else None
                sl_ec = sl[1] if sl else None
                
                profit = 0
                if sl_ec is not None:
                    # if stop loss triggers
                    profit = slo_ec - sl_ec
                    total_profit += profit * 100
                else:
                    profit = slo_ec
                    total_profit += profit * 100
            
                # debug
                #print("Date:", date)
                #print(f"Call: {is_call}")
                #print(f"Spread: {lower_strike, upper_strike, spread_credit}")
                #print(f"SLO: {slo}")
                #print(f"SL: {sl}")
                #print(f"Exit credit: {sl_ec if sl is not None else None}")
                #print(f"Profit: {profit}")
                #print("")
    return total_profit

def thread_worker(date, timestamp_of_entry, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):
    # get spreads
    call_spreads, put_spreads = find_strikes(date, timestamp_of_entry, entry_credit, spread_width, num_spreads)
    spreads = call_spreads + put_spreads
    
    total_wins = 0
    total_losses = 0
    total_profit = 0
    
    for i, (lower_strike, upper_strike, spread_credit) in enumerate(spreads):
        is_call = i < len(call_spreads)
        
        # process spread
        slo = stop_limit_order(date, lower_strike, upper_strike, timestamp_of_entry, stop_price, limit_price, is_call)
        
        slo_et = slo[0] if slo else None # exit time
        slo_ec = slo[1] if slo else None # execution credit
        
        # if entered, attach stop loss
        if slo_et is not None:
            sl = stop_loss(date, lower_strike, upper_strike, slo_et, slo_ec, sl_mult, is_call)
            
            sl_et = sl[0] if sl else None
            sl_ec = sl[1] if sl else None
            
            profit = 0
            if sl_ec is not None:
                # if stop loss triggers
                profit = slo_ec - sl_ec
                total_profit += profit * 100
                total_losses += 1
            else:
                profit = slo_ec
                total_profit += profit * 100
                total_wins += 1
    return total_profit

def process_spreads_multithreaded(timestamp_of_entry, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 3, 1)
    
    # list of dates to backtest
    date_range = [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date - start_date).days + 1)]
    directory = 'processed-data'
    files = os.listdir(directory)
    filtered_dates = [file for file in files if file in date_range]

    # thread for each day
    with ThreadPoolExecutor() as executor:
        results = executor.map(thread_worker, filtered_dates, [timestamp_of_entry]*len(filtered_dates), 
                               [spread_width]*len(filtered_dates), [entry_credit]*len(filtered_dates), 
                               [num_spreads]*len(filtered_dates), [stop_price]*len(filtered_dates), 
                               [limit_price]*len(filtered_dates), [sl_mult]*len(filtered_dates))

    total_profit = 0
    for profit in results:
        total_profit += profit
    return total_profit

def manual_input():
    profit = process_spreads(timestamp_of_entry=100000000, spread_width=30, entry_credit=1.5, num_spreads=3, stop_price=1.1, limit_price=1, sl_mult=2.0)
    print(f"Total profit: {profit}")

# for backtest
def argument_input(entry_timestamp, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):
    profit = process_spreads_multithreaded(entry_timestamp, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult)
    print(f"Total profit: {profit}")
    return profit

def test():
    profit = argument_input(100000000, 30, 1.5, 3, 1.1, 1, 2.0)
    print(f"Total profit: {profit}")

# TESTING -----------------------------------------------------------------------------------------------------------------------
#start_time = time.time()
print(find_spread_search_range("20231101"))
# performance analysis
profiler = cProfile.Profile()
profiler.runcall(test) # multi-threaded
#profiler.runcall(manual_input) # single thread
profiler.print_stats()

#print(manual_input())
#print(find_strikes("20230412", 100000000, 1.5, 30, 1, True))
#print(find_strikes("20230412", 100000000, 1.5, 30, 3, False))
#print(stop_limit_order("20230412", 4140, 4170, 100000000, 1.1, 1, True))
#print(stop_loss("20230412", 4135, 4165, 120000000, 1.08, 2, True))

#end_time = time.time()
#print(f"Execution Time is {end_time - start_time} seconds!")

'''
LOGIC:
create list of dates between start and end date
filter to only include dates with data

for each date:
    get call and put spreads based on date and entry credit
    
    for each spread:
        attach stop limt order
        if stop limit order triggers
            attach stop loss
            if triggered
                profit = stop limit order execution credit - stop loss execution credit (loss)
            else
                profit = stop limit order execution credit (win)
                
        add to total profit
'''

'''
--------------------------------------------LEGACY FUNCTIONS---------------------------------------------------------------------
'''
def get_mid_price(day, table_name, timestamp):
    global exe_path
    
    date = day
    strike_price = str(table_name)
    option = "C"
    time_str = str(timestamp)
    
    args = [date, strike_price, option, time_str]
    cmd = [exe_path] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    tokens = result.stdout.split()
    if tokens is not None and len(tokens) >= 2:
        return float(tokens[1]) if tokens[1] is not None else 0
    else:
        return None
    
#Returns the credit received as well as the lower and upper option prices
def calculate_credit_received(day, lower_strike, upper_strike, timestamp, is_call):
    lower_strike_table_name = ('C' if is_call else 'P') + str(lower_strike)
    upper_strike_table_name = ('C' if is_call else 'P') + str(upper_strike)
        
    lower_option_price = get_mid_price(day, lower_strike_table_name, timestamp)
    upper_option_price = get_mid_price(day, upper_strike_table_name, timestamp)
    
    # Calculate the credit received from the spread
    if lower_option_price is not None and upper_option_price is not None:
        credit_received = round(lower_option_price - upper_option_price if is_call else upper_option_price - lower_option_price, 3)
        return (credit_received, lower_option_price, upper_option_price)
    return (None, None, None)

#Finding strikes to monitor based on entry credit ($1.5 for example)
def find_call_strikes(day, timestamp_of_entry, entry_credit, spread_width, num_spreads):
    spreads = []

    for short_strike in range(4225, 4000, -5):
        long_strike = short_strike + spread_width
        credit_received = calculate_credit_received(day, short_strike, long_strike, timestamp_of_entry, True)[0]    
        
        if credit_received is not None and credit_received >= entry_credit:
            spreads.append((short_strike, long_strike, credit_received))
            if len(spreads) == num_spreads:
                return spreads     
    return spreads
        
def find_put_strikes(day, timestamp_of_entry, entry_credit, spread_width, num_spreads):
    spreads = []

    for long_strike in range(3900, 4200, 5):
        short_strike = long_strike + spread_width
        credit_received = calculate_credit_received(day, long_strike, short_strike, timestamp_of_entry, False)[0]
        
        if credit_received is not None and credit_received >= entry_credit:
            spreads.append((long_strike, short_strike, credit_received))
            if len(spreads) == num_spreads:
                return spreads
    return spreads

def find_spread_search_range(day):
    path = "processed-data/" + day
    dir_list = os.listdir(path) 

    for index, table_name in enumerate(dir_list):
        next_table_name = dir_list[index + 1]
        strike = int(table_name[1:])
        next_strike = int(next_table_name[1:])

        if next_strike - strike == 5:
            search_lower_bound = strike
            break

    for i in range(len(dir_list) - 1, 0, -1):
        table_name = dir_list[i]
        previous_table_name = dir_list[i-1]
        strike = int(table_name[1:])
        previous_strike = int(previous_table_name[1:])

        if strike - previous_strike == 5:
            search_upper_bound = strike
            return (search_lower_bound, search_upper_bound)

# finding strikes to monitor based on entry credit ($1.5 for example)
def find_strikes_old(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, is_call, lower, upper):
    global exe_path
    
    # convert arguments to strings
    args = [str(arg) for arg in [date, timestamp_of_entry, spread_width, entry_credit, num_spreads, lower, upper]]
    args.append("call" if is_call else "put")
    cmd = [exe_path] + args

    # run executable
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.split("\n")
    
    # parse output to list
    spreads = [(int(tokens[0]), int(tokens[1]), float(tokens[2])) for tokens in (line.split() for line in lines) if tokens and len(tokens) >= 3]
    return spreads or []

#print(get_mid_price("20230412", "P4055", 123000000))
#print(find_call_strikes("20230410", timestamp_of_entry=100000000, entry_credit=1.5, spread_width=30, num_spreads=3))
#print(find_put_strikes("20230410", timestamp_of_entry=100000000, entry_credit=1.5, spread_width=30, num_spreads=3))
#print(find_spread_search_range("20230412"))