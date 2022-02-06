#!/home/mice/anaconda3/envs/wealthtracker/bin/python

import psycopg2 as psql
import pandas as pd
import configparser
import requests
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np


def chart_cashflows(cfs):
    plt.bar(cfs.keys(), cfs.values())
    plt.show()


class Cashflows:
    def __init__(self, configfile):
        self.accounts_cols = ['account_id', 'account_type', 'bank', 'status', 'joint']
        self.transactions_cols = ['id', 'account_id', 'transaction_date', 'category', 'description', 'amount']
        self.expense_types_cols = ['id', 'category', 'target']

        psql_config = configparser.ConfigParser()
        psql_config.read(configfile)
        local_ip = psql_config['myip']['localip']
        current_ip = requests.get('https://api.ipify.org/').text
        location = 'outside' if local_ip != current_ip else ''
        conn_location = ''.join(['postgresql', location])
        self.conn = psql.connect(host=psql_config[conn_location]['host'],
                                 port=psql_config[conn_location]['port'],
                                 database=psql_config[conn_location]['database'],
                                 user=psql_config[conn_location]['user'],
                                 password=psql_config[conn_location]['password'])

    def execute_command(self, command):
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(command)
                return curs.fetchall()

    def get_table_range(self, table, date_cat, fro, to):
        command = f"""SELECT * from {table} 
                        WHERE {date_cat} BETWEEN '{fro}'::date AND '{to}'::date 
                        ORDER BY {date_cat}"""
        return self.execute_command(command)

    def get_cashflows(self, fro=None, to=None):
        tz = 'America/Edmonton'
        # Convert fro and to to datetime
        fro = pd.Timestamp('2021-04-01', tz=tz) if fro is None else pd.Timestamp(fro, tz=tz)
        to = pd.Timestamp(datetime.now().date(), tz=tz) if to is None else pd.Timestamp(to, tz=tz)

        # We need to put the start and end dates to first and last of each respective month,
        # not where indicated by fro and to. Manipulate the dates to get those dates
        monthly = pd.date_range(start=fro, end=to, freq='M', tz=tz)
        dates_start = []
        dates_end = []
        # Append the first day of the month of 'fro', in case 'fro' was entered in the middle
        dates_start.append(fro - timedelta(days=(fro.day - 1)))
        for date in monthly:
            dates_end.append(date)
            dates_start.append(date + timedelta(days=1))
        # Append the last day of the month of 'to',  not 'to' itself. This will prevent incomplete cashflow calculations
        dates_end.append(pd.date_range(start=to, end=to + timedelta(days=(32 - to.day)),
                                       freq='M', tz=tz)[0])

        # Retrieve transaction data based on calculated dates and input to DataFrame
        df = pd.DataFrame(self.get_table_range('transactions', 'transaction_date', dates_start[0], dates_end[-1]),
                          columns=self.transactions_cols)
        df.drop(columns='id', inplace=True)  # Drop id, it's useless
        # Covert dates to datetime type with local tz
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.tz_localize(tz)

        # # Self Generated Histogram
        # cashflows = {}
        # for start, end in zip(dates_start, dates_end):
        #     # We want to mask by dates, and filter out expense_types: credit_payments, admin, ignore
        #     mask = (df['transaction_date'] >= start) & (df['transaction_date'] <= end) & \
        #            (df['category'] != 'admin') & \
        #            (df['category'] != 'credit_payments') & \
        #            (df['category'] != 'ignore')
        #
        #     # Create dict entry by Month Year and net cashflow
        #     cashflows[start.strftime('%B %Y')] = df[mask]

        # Generate Histogram
        hist_bin = dates_start + [dates_end[-1]]  # Generate bins for histogram

        # Generate Masks and get raw data in one dataframe
        mask = (df['category'] != 'admin') & \
               (df['category'] != 'credit_payments') & \
               (df['category'] != 'ignore')
        return df[mask], hist_bin

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


class Transactions:
    def __init__(self, configfile):
        self.expense_types = None
        self.expense_specifics = None
        self.cat_path = "categories.cfg"
        self.refresh_categories()

        self.accounts_cols = ['account_id', 'account_type', 'bank', 'status', 'joint']
        self.transactions_cols = ['id', 'account_id', 'transaction_date', 'category', 'description', 'amount']
        self.expense_types_cols = ['id', 'category', 'target']

        psql_config = configparser.ConfigParser()
        psql_config.read(configfile)
        local_ip = psql_config['myip']['localip']
        current_ip = requests.get('https://api.ipify.org/').text
        location = 'outside' if local_ip != current_ip else ''
        conn_location = ''.join(['postgresql', location])
        self.conn = psql.connect(host=psql_config[conn_location]['host'],
                                 port=psql_config[conn_location]['port'],
                                 database=psql_config[conn_location]['database'],
                                 user=psql_config[conn_location]['user'],
                                 password=psql_config[conn_location]['password'])

    def refresh_categories(self):
        cat_cfg = configparser.ConfigParser()
        cat_cfg.read(self.cat_path)
        self.expense_types = list(cat_cfg['Types'])
        self.expense_specifics = dict(cat_cfg['Specifics'])

    def update_categories(self, line):
        with open(self.cat_path, 'a') as file:
            file.write(line + '\n')

    def execute_command(self, command):
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(command)
                return curs.fetchall()

    def resolve_uncategorized(self):
        """ User to resolve any uncategorized Transactions in Database """
        try:
            command = "SELECT * FROM transactions WHERE category = 'uncategorized';"
            curr_uncat = self.execute_command(command)
            for trans in curr_uncat:
                chosen_cat = input(f"Choose for {trans[-2]} -> {trans[-1]}\nFrom {self.expense_types}\nInput -> ")
                new_cat = [categ for categ in self.expense_types if categ in chosen_cat]
                command = f"UPDATE transactions SET category = '{new_cat[0]}' WHERE id = {trans[0]} RETURNING *"
                self.execute_command(command)
                to_update = input(f"Choose to update categories? [Y/N]")
                if 'y' in to_update.lower():  # Need to update list
                    chosen_keyword = input("Choose Keyword to add: ")
                    self.update_categories(f"{chosen_keyword} = {chosen_cat}")
                    self.refresh_categories()
        except Exception as e:
            print(e)

    def insert_transactions(self):
        """ Insert Transactions into Database using CSV in transaction_records folder
            Name of CSV shall be:
                'AccountNumber'.csv
            Structure of CSV shall be:
                Date | Description | Debit Amount | Credit Amount
                No header line in CSV
        """

        try:
            all_files = os.listdir("transaction_records")
            for each_file in all_files:
                file_path = os.path.join("transaction_records", each_file)
                with open(file_path, 'r') as file:
                    for line in file.readlines():
                        transaction = [itm for itm in line.strip().split(',')]
                        transaction[1] = transaction[1].replace("'", "").strip()  # Cleanup
                        account_id = each_file.split('.')[0]
                        transaction_date = datetime.strptime(transaction[0], '%m/%d/%Y')
                        index = [cat for cat in self.expense_specifics.keys() if cat in transaction[1].lower()]
                        category = self.expense_specifics[index[0]] if len(index) > 0 else 'uncategorized'
                        description = transaction[1]
                        if len(transaction[2]) > 0:  # Debit, use negative number
                            amount = -1 * float(transaction[2])
                        elif len(transaction[3]) > 0:  # Credit, use positive number
                            amount = float(transaction[3])
                        else:
                            amount = 0.0

                        command = f"""INSERT INTO transactions (account_id,transaction_date,category,description,amount)
                                    VALUES ('{account_id}','{transaction_date}','{category}','{description}','{amount}')
                                    RETURNING *"""
                        self.execute_command(command)
                # Delete transaction file as its no longer needed
                os.remove(file_path)

                # Update last_updated date in table accounts
                command = f"""SELECT transaction_date from transactions WHERE account_id = '{account_id}'
                                ORDER BY transaction_date desc limit 1"""
                last_date, = self.execute_command(command).pop()
                command = f"""UPDATE accounts 
                                SET last_updated = '{last_date}'::date 
                                WHERE account_id = '{account_id}' 
                                RETURNING *"""
                self.execute_command(command)

        except Exception as e:
            print(e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def main():
    pass


if __name__ == '__main__':
    main()
