import csv
import datetime
import json
import os
import re
from itertools import groupby
from os import path

from beancount.ingest.importer import ImporterProtocol
from beancount.core.amount import Amount
from beancount.core.data import EMPTY_SET, new_metadata, Cost, Posting, Price, Transaction
from beancount.core.number import D, round_to


class Importer(ImporterProtocol):

    def __init__(self, currency, account):
        self.currency = currency
        self.account = account

    def name(self) -> str:
        return 'Kraken'

    def identify(self, file) -> bool:
        return (re.match("ledgers.csv", path.basename(file.name)) and
                re.match("\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"asset\",\"amount\",\"fee\",\"balance\"", file.head()))

    def extract(self, file, existing_entries=None) -> list:
        with open(file.name, 'r') as _file:
            transactions = list(csv.DictReader(_file))

        entries = []

        sorted_transactions = sorted(
            transactions,
            key=lambda tx: (tx['time'], tx['type']),
        )
        currency_map = {
            "XETC": "ETC",
            "XETH": "ETH",
            "XLTC": "LTC",
            "XNMC": "NMC",
            "XXBT": "BTC",
            "XXDG": "DOGE",
            "XXLM": "XLM",
            "ZUSD": "USD",
        }
        transactions_by_ref = groupby(
            transactions,
            lambda tx: tx['refid'],
        )
        for ref_id, transfers in transactions_by_ref:
            increase_amount = 0
            increase_currency = None
            increase_fee_amount = 0
            postings = []
            reduce_amount = 0
            reduce_currency = None
            reduce_fee_amount = 0
            title = ''
            trade_type = None
            tx_date = None

            for transfer in transfers:
                trade_type = transfer['type']
                tx_date = datetime.datetime.strptime(transfer['time'], "%Y-%m-%d %H:%M:%S")
                amount = D(transfer['amount'])
                currency = currency_map.get(transfer['asset']) or transfer['asset']
                fee = D(transfer['fee'])
                if amount > 0:
                    increase_amount = amount
                    increase_currency = currency
                    increase_fee_amount = fee
                else:
                    reduce_amount = amount
                    reduce_currency = currency
                    reduce_fee_amount = fee
                account = f'{self.account}:{currency}'
                metadata = {'refid': transfer['refid']}
                title = f'{trade_type.capitalize()} {currency}'
                if transfer['txid'] != "":
                    metadata['transferid'] = transfer['txid']

            account = f'Assets:Wallets:{currency}'

            if trade_type == 'deposit':
                postings.append(Posting(
                    account,
                    Amount(-increase_amount, increase_currency),
                    None, None, None, None,
                ))
                postings.append(Posting(
                    f'Assets:Kraken:{currency}',
                    Amount(increase_amount, increase_currency),
                    None, None, None, None,
                ))

            if trade_type == 'trade':
                if reduce_amount:
                    if reduce_fee_amount:
                        postings.append(Posting(
                            f'Assets:Kraken:{reduce_currency}',
                            Amount(-reduce_fee_amount, reduce_currency),
                            None, None, None, None,
                        ))
                        postings.append(Posting(
                            f'Expenses:Kraken:{reduce_currency}:Fees',
                            Amount(reduce_fee_amount, reduce_currency),
                            None, None, None, None,
                        ))
                    postings.append(Posting(
                        f'Assets:Kraken:{reduce_currency}',
                        Amount(reduce_amount, reduce_currency),
                        None, None, None, None,
                    ))

                if increase_amount:
                    postings.append(Posting(
                        f'Assets:Kraken:{increase_currency}',
                        Amount(increase_amount, increase_currency),
                        None, None, None, None,
                    ))
                    if increase_fee_amount:
                        postings.append(Posting(
                            f'Assets:Kraken:{increase_currency}',
                            Amount(increase_fee_amount, increase_currency),
                            None, None, None, None,
                        ))
                        postings.append(Posting(
                            f'Expenses:Kraken:{increase_currency}:Fees',
                            Amount(-increase_fee_amount, increase_currency),
                            None, None, None, None,
                        ))

                postings.append(
                    Posting('Income:Kraken:PnL', None, None, None, None, None)
                )


            if trade_type == 'withdrawal':
                postings.append(Posting(
                    f'Assets:Kraken:{reduce_currency}',
                    Amount(reduce_amount, reduce_currency),
                    None, None, None, None,
                ))
                postings.append(Posting(account, None, None, None, None, None))

            if trade_type == 'transfer':
                if increase_amount:
                    postings.append(Posting(
                        f'Assets:Kraken:{increase_currency}',
                        Amount(increase_amount, increase_currency),
                        None, None, None, None,
                    ))

                if reduce_amount:
                    postings.append(Posting(
                        f'Assets:Kraken:{reduce_currency}',
                        Amount(reduce_amount, reduce_currency),
                        None, None, None, None,
                    ))

            entry = Transaction(
                new_metadata(file.name, 0, metadata),
                tx_date.date(),
                '*',
                title,
                '',
                EMPTY_SET,
                EMPTY_SET,
                postings,
            )

            entries.append(entry)


        return entries
