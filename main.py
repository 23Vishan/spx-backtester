import os
import subprocess
from datetime import datetime, timedelta
import subprocess
from concurrent.futures import ThreadPoolExecutor
import re
import pandas as pd

exe_path = "F:/Workplace/Compression.exe"

# format time
def convert_to_seconds(timestamp):
    hours = int(timestamp[:2])
    minutes = int(timestamp[2:4])
    seconds = int(timestamp[4:6])
    milliseconds = int(timestamp[6:])
    total_seconds = hours * 3600 + minutes * 60 + seconds + milliseconds / 1000.0
    return total_seconds

# format stoploss and stoplimitorder
def format_data(data):
    if data is not None and None not in data:
        tmp_timestamp, tmp_value = data
        tmp_timestamp = convert_to_seconds(str(tmp_timestamp))
        tmp_timestamp = (datetime.min + timedelta(seconds=tmp_timestamp)).time()
        tmp_timestamp = tmp_timestamp.strftime('%H:%M')
        tmp_value = "$" + "{:.3f}".format(tmp_value)
    else:
        tmp_timestamp, tmp_value = 'None', 'None'
    return tmp_timestamp, tmp_value

def stop_limit_order(day, lower_strike, upper_strike, timestamp, stop_price, limit_price, is_call):
    global exe_path
    
    date = day
    lower_strike_table_name = ('C' if is_call else 'P') + str(lower_strike)
    upper_strike_table_name = ('C' if is_call else 'P') + str(upper_strike)
    entry_time = str(timestamp)
    stop_price = str(stop_price)
    limit_price = str(limit_price)
    option = "stoplimitorder"
    
    args = [date, lower_strike_table_name, upper_strike_table_name, entry_time, stop_price, limit_price, option]
    cmd = [exe_path] + args

    # Use subprocess.Popen with the creationflags parameter
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)
    stdout, stderr = process.communicate()
    result = stdout.decode('utf-8')

    tokens = result.split()
    if tokens is not None and len(tokens) >= 2:
        exit_time = int(tokens[0]) if tokens[0] is not None else 0
        rounded_value_of_position = round(float(tokens[1]), 2) if tokens[1] is not None else 0
        return(exit_time, rounded_value_of_position)
    else:
        return None, None

def stop_loss(day, lower_strike, upper_strike, timestamp, entry_credit, stop_multiplier, is_call):
    global exe_path
    
    date = day
    lower_strike_table_name = ('C' if is_call else 'P') + str(lower_strike)
    upper_strike_table_name = ('C' if is_call else 'P') + str(upper_strike)
    entry_time = str(timestamp)
    entry_credit = str(entry_credit)
    stop_multiplier = str(stop_multiplier)
    option = "stoploss"
    
    args = [date, lower_strike_table_name, upper_strike_table_name, entry_time, entry_credit, stop_multiplier, option]
    cmd = [exe_path] + args

    # Use subprocess.Popen with the creationflags parameter
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)
    stdout, stderr = process.communicate()
    result = stdout.decode('utf-8')

    tokens = result.split()
    if tokens is not None and len(tokens) >= 2:
        exit_time = int(tokens[0]) if tokens[0] is not None else 0
        rounded_value_of_position = round(float(tokens[1]), 2) if tokens[1] is not None else 0
        return(exit_time, rounded_value_of_position)
    else:
        return None, None

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
def find_strikes(date, timestamp_of_entry, entry_credit, spread_width, num_spreads):
    global exe_path
    
    search = find_spread_search_range(date)
    search_lower = str(search[0])
    search_upper = str(search[1])
    
    args = [str(arg) for arg in [date, timestamp_of_entry, spread_width, entry_credit, num_spreads, search_lower, search_upper]]
    args.append("both")
    cmd = [exe_path] + args

    # Use subprocess.Popen with the creationflags parameter
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)
    stdout, stderr = process.communicate()
    result = stdout.decode('utf-8')
    parts = result.split("break")

    # split data into calls and puts
    calls = parts[0].strip().split('\n')
    puts = parts[1].strip().split('\n')

    # parse output to list
    calls_spreads = [(int(tokens[0]), int(tokens[1]), float(tokens[2])) for tokens in (line.split() for line in calls) if tokens and len(tokens) >= 3]
    puts_spreads = [(int(tokens[0]), int(tokens[1]), float(tokens[2])) for tokens in (line.split() for line in puts) if tokens and len(tokens) >= 3]
    return calls_spreads or [], puts_spreads or []

def thread_worker(date, timestamp_of_entry, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):
    win, loss = [], []
    win_count, loss_count = 0, 0
    call_win_count, call_loss_count, put_win_count, put_loss_count = 0, 0, 0, 0
    trading_log_buffer, total_daily_win_loss_buffer, daily_data_buffer = [], [], []
    
    # find spreads
    call_spreads, put_spreads = find_strikes(date, timestamp_of_entry, entry_credit, spread_width, num_spreads)
    spreads = call_spreads + put_spreads
    
    # process all spreads
    daily_profit, daily_loss, daily_win = 0, 0, 0
    total_call_profit, total_call_loses, total_put_profit, total_put_loses = 0, 0, 0, 0
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
                daily_loss += profit * 100
                loss_count += 1
                loss.append(profit)
                
                if is_call:
                    call_loss_count += 1
                    total_call_loses += profit
                else:
                    put_loss_count += 1
                    total_put_loses += profit
            else:
                profit = slo_ec
                daily_win += profit * 100
                win_count += 1
                win.append(profit)
                
                if is_call:
                    call_win_count += 1
                    total_call_profit += profit
                else:
                    put_win_count += 1
                    total_put_profit += profit
            daily_profit += profit * 100
                
            # update trading log
            slo_et_formatted, slo_ec_formatted = format_data(slo)
            sl_et_formated, sl_ec_formatted = format_data(sl)                    
            date_formatted = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
            call_or_put = 'Call' if is_call else 'Put'
            spread_credit_formatted = "${:.3f}".format(spread_credit)
            profit_formatted = "{:.3f}".format(profit)
            trading_log_buffer.append((date_formatted, f"{date_formatted} {call_or_put} {lower_strike}/{upper_strike} {spread_credit_formatted} {slo_et_formatted} {slo_ec_formatted} {sl_et_formated} {sl_ec_formatted} {profit_formatted}\n"))
    
    # daily data
    date_formatted = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
    win_rate = win_count / (win_count + loss_count) if win_count + loss_count != 0 else 0
    daily_data_buffer.append((date_formatted, f"{date_formatted} {round(daily_profit, 2)} {call_win_count}/{call_loss_count} {round(total_call_profit, 2)}/{round(total_call_loses, 2)} {put_win_count}/{put_loss_count} {round(total_put_profit, 2)}/{round(total_put_loses, 2)} {win_rate} {round(daily_win, 2)} {round(abs(daily_loss), 2)}\n"))

    # update other metrics
    total_daily_win_loss_buffer.append((date, f"{date} {round(daily_win, 2)} {round(abs(daily_loss), 2)}\n"))
    return daily_profit, trading_log_buffer, total_daily_win_loss_buffer, win_count, loss_count, call_win_count, call_loss_count, put_win_count, put_loss_count, win, loss, daily_data_buffer

def process_spreads_multithreaded(start_date_str, end_date_str, timestamp_of_entry, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):    
    # clear files
    filenames = ['user-interface/TradingLog.txt', 'user-interface/PnL.txt', 'user-interface/TotalDailyWinLoss.txt', 'user-interface/DailyData.txt']
    for filename in filenames:
        with open(filename, 'w') as f:
            f.write("")
            
    # list of dates to backtest
    start_date = datetime.strptime(str(int(start_date_str)), '%Y%m%d')
    end_date = datetime.strptime(str(int(end_date_str)), '%Y%m%d')
    date_range = [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date - start_date).days + 1)]
    directory = 'processed-data'
    files = os.listdir(directory)
    filtered_dates = [file for file in files if file in date_range]

    # create thread for each day
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(thread_worker, filtered_dates, [timestamp_of_entry]*len(filtered_dates), 
                               [spread_width]*len(filtered_dates), [entry_credit]*len(filtered_dates), 
                               [num_spreads]*len(filtered_dates), [stop_price]*len(filtered_dates), 
                               [limit_price]*len(filtered_dates), [sl_mult]*len(filtered_dates)))

        profits = [result[0] for result in results]
        trading_log_buffers = [result[1] for result in results]
        total_daily_win_loss_buffers = [result[2] for result in results]
        wins = [result[3] for result in results]
        losses = [result[4] for result in results]
        call_wins = [result[5] for result in results]
        call_losses = [result[6] for result in results]
        put_wins = [result[7] for result in results]
        put_losses = [result[8] for result in results]
        win = [result[9] for result in results]
        loss = [result[10] for result in results]
        daily_data_buffers = [result[11] for result in results]
                
        total_profit = sum(profits)
        max_daily_win = max(profits)
        max_daily_loss = min(profits)
        win_count = sum(wins)
        loss_count = sum(losses)
        call_win_count = sum(call_wins)
        call_loss_count = sum(call_losses)
        put_win_count = sum(put_wins)
        put_loss_count = sum(put_losses)
        total_money_won = sum(item for sublist in win for item in sublist)
        total_money_lost = sum(item for sublist in loss for item in sublist)
                
    # sort by date
    for buffer in [trading_log_buffers, total_daily_win_loss_buffers, daily_data_buffers]:
        buffer.sort()
    
    # write to files
    with open('user-interface/TradingLog.txt', 'a') as f:
        for buffer in trading_log_buffers:
            for _, line in buffer:
                f.write(line)
    with open('user-interface/TotalDailyWinLoss.txt', 'a') as f:
        for buffer in total_daily_win_loss_buffers:
            for _, line in buffer:
                f.write(line)
    with open('user-interface/PnL.txt', 'a') as f:
        profit_to_date = 0
        for date, profit in zip(filtered_dates, profits):
            profit_to_date += profit
            profit_to_date = round(profit_to_date, 2)
            f.write(f"{date} {profit_to_date}\n")
    with open('user-interface/DailyData.txt', 'a') as f:
        for buffer in daily_data_buffers:
            for _, line in buffer:
                f.write(line)

    longest_win_streak, longest_loss_streak = 0, 0            
    consecutive_wins, consecutive_losses = 0, 0
    winning_days, losing_days = 0, 0
    total_win, total_loss = 0, 0
    with open('user-interface/TotalDailyWinLoss.txt', 'r') as f:
        for line in f:
            _, win, loss = line.split()
            if float(win) > float(loss):
                winning_days += 1
                total_win += float(win)
                consecutive_wins += 1
                consecutive_losses = 0
                longest_win_streak = max(longest_win_streak, consecutive_wins)
            else:
                losing_days += 1
                total_loss += float(loss)
                consecutive_losses += 1
                consecutive_wins = 0
                longest_loss_streak = max(longest_loss_streak, consecutive_losses)
    average_winning_day = round(total_win / winning_days, 2) if winning_days != 0 else 0
    average_losing_day = round(total_loss / losing_days, 2) if losing_days != 0 else 0
    
    average_win = round(total_money_won / win_count, 2) if win_count != 0 else 0
    average_loss = round(total_money_lost / loss_count, 2) if loss_count != 0 else 0
    win_rate = win_count / (win_count + loss_count) if win_count + loss_count != 0 else 0
        
    # rounding
    win_rate = round(win_rate, 4)
    max_daily_win = round(max_daily_win, 2)
    max_daily_loss = round(max_daily_loss, 2)
    return float(round(total_profit, 2)), float(win_count), float(loss_count), float(win_rate), float(max_daily_win), float(max_daily_loss), float(call_win_count), float(call_loss_count), float(put_win_count), float(put_loss_count), float(winning_days), float(losing_days), float(average_winning_day), float(average_losing_day), float(longest_win_streak), float(longest_loss_streak), float(average_win), float(average_loss)

'''
THESE FUNCTIONS ARE DESIGNED TO BE CALLED FROM MATLAB
'''
# finding strikes to monitor based on entry credit ($1.5 for example)
def find_strikes_ui(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, is_call):
    global exe_path
    
    date = str(int(date))
    search = find_spread_search_range(date)
    
    d = date
    et = str(timestamp_of_entry)
    sw = str(spread_width)
    ec = str(entry_credit)
    ns = str(num_spreads)
    search_lower = str(search[0])
    search_upper = str(search[1])
    option = "call" if is_call else "put"
    
    args = [d, et, sw, ec, ns, search_lower, search_upper, option]
    cmd = [exe_path] + args

    # Use subprocess.Popen with the creationflags parameter
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)

    stdout, stderr = process.communicate()
    result = stdout.decode('utf-8')

    lines = result.split("\n")
    for line in lines:
        tokens = line.split()
        if tokens is not None and len(tokens) >= 3:
            lower_strike = int(tokens[0])
            upper_strike = int(tokens[1])
            credit_received = float(tokens[2])
            
            with open('user-interface/FindSpreads.txt', 'a') as f:
                f.write(f"{lower_strike} {upper_strike} {credit_received}\n")
                f.flush()

def stop_loss_ui(day, lower_strike, upper_strike, timestamp, entry_credit, stop_multiplier, is_call):
    global exe_path
    
    day = str(int(day))

    date = day
    lower_strike_table_name = ('C' if is_call else 'P') + str(int(lower_strike))
    upper_strike_table_name = ('C' if is_call else 'P') + str(int(upper_strike))
    entry_time = str(int(timestamp))
    entry_credit = str(entry_credit)
    stop_multiplier = str(stop_multiplier)
    option = "stoploss"
    
    args = [date, lower_strike_table_name, upper_strike_table_name, entry_time, entry_credit, stop_multiplier, option]
    cmd = [exe_path] + args

    # Use subprocess.Popen with the creationflags parameter
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)

    stdout, stderr = process.communicate()
    result = stdout.decode('utf-8')

    tokens = result.split()
    if tokens is not None and len(tokens) >= 2:
        exit_time = int(tokens[0]) if tokens[0] is not None else 0
        rounded_value_of_position = round(float(tokens[1]), 2) if tokens[1] is not None else 0
        
        with open('user-interface/StopLoss.txt', 'a') as f:
            f.write(f"{exit_time} {rounded_value_of_position}\n")
            f.flush()
    else:
        with open('user-interface/StopLoss.txt', 'a') as f:
            f.write(f"None None\n")
            f.flush()

def stop_limit_order_ui(day, lower_strike, upper_strike, timestamp, stop_price, limit_price, is_call):
    global exe_path
    
    day = str(int(day))

    date = day
    lower_strike_table_name = ('C' if is_call else 'P') + str(int(lower_strike))
    upper_strike_table_name = ('C' if is_call else 'P') + str(int(upper_strike))
    entry_time = str(int(timestamp))
    stop_price = str(stop_price)
    limit_price = str(limit_price)
    option = "stoplimitorder"
    
    args = [date, lower_strike_table_name, upper_strike_table_name, entry_time, stop_price, limit_price, option]
    cmd = [exe_path] + args

    # Use subprocess.Popen with the creationflags parameter
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)

    stdout, stderr = process.communicate()
    result = stdout.decode('utf-8')

    tokens = result.split()
    if tokens is not None and len(tokens) >= 2:
        exit_time = int(tokens[0]) if tokens[0] is not None else 0
        rounded_value_of_position = round(float(tokens[1]), 2) if tokens[1] is not None else 0
        
        with open('user-interface/StopLimitOrder.txt', 'a') as f:
            f.write(f"{exit_time} {rounded_value_of_position}\n")
            f.flush()
    else:
        with open('user-interface/StopLimitOrder.txt', 'a') as f:
            f.write(f"None None\n")
            f.flush()

def print_file_ui(date, table_name, is_call):
    global exe_path
    
    date = str(int(date))
    strike_price = ('C' if is_call else 'P') + str(int(table_name))
    option = "A"
    time_str = ""
    
    args = [date, strike_price, option, time_str]
    cmd = [exe_path] + args
    
    with open('user-interface/PrintFile.txt', 'w') as f:
        process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)
        stdout, stderr = process.communicate()
        
def plot_file_ui(date, table_name, is_call):
    global exe_path
    
    date = str(int(date))
    strike_price = ('C' if is_call else 'P') + str(int(table_name))
    option = "A"
    time_str = ""
    
    args = [date, strike_price, option, time_str]
    cmd = [exe_path] + args
    
    times = []
    mids = []
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)
    stdout, stderr = process.communicate()
    
    for line in stdout.decode('utf-8').split('\n'):
        match = re.search(r'Time: (\d+), Mid: ([\d.]+)', line)
        if match:
            time, mid = match.groups()
            times.append(time)
            mids.append(mid)
    df = pd.DataFrame({'Time': times, 'Mid': mids})
    
    if df.empty:
        open('user-interface/PlotFile.txt', 'w').close()
        return

    df['Time'] = pd.to_datetime(df['Time'], format='%H%M%S%f')
    df = df.set_index('Time')
    if len(df) > 1:
        df_resampled = df.resample('500ms').ffill()
        df_resampled = df_resampled.between_time('9:30', '16:00')
        df_resampled = df_resampled.iloc[1:]
    else:
        index = pd.date_range(start='9:30', end='16:00', freq='500ms')
        df_resampled = pd.DataFrame(index=index)
        df_resampled['Mid'] = df['Mid'].iloc[0]

    df_resampled.index = df_resampled.index.strftime('%H:%M:%S:%f')
    df_resampled.to_csv('user-interface/PlotFile.txt', sep=' ', header=False)

def test():
    result = process_spreads_multithreaded(start_date_str="20230101", end_date_str="20230201", timestamp_of_entry=100000000, spread_width=30, entry_credit=1.5, num_spreads=3, stop_price=1.1, limit_price=1, sl_mult=2.0)
    print(result)

#profiler = cProfile.Profile()
#profiler.runcall(test)
#profiler.print_stats()
#test()

#print(find_strikes_ui(20230103, 100000000, 1.5, 30, 3, 1))
#print(stop_loss_ui(20230103, 3875, 3905, 100000000, 1.0, 2.0, 1))
#print(stop_limit_order_ui(20230103, 3875, 3905, 100000000, 1.1, 1, 1))
#print(print_file_ui(20231023, 4540, 0))
#print(print_file_ui(20231023, 4210, 1))
#plot_file_ui(20230103, 4000, 1)