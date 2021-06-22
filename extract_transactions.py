import requests
import os
import pandas as pd
from datetime import datetime
import csv


def main(in_class="mined", out_class="remove funds"):
    """

    Args:
        in_class (str): Default classification for incoming transactions
        out_class (str): Default classification for outgoing transactions
    """

    wallet = os.environ["ETH_WALLET"]
    csv_path = "transactions.csv"

    inc = 100
    n = 0
    new_c = 0

    while True:
        # Try to read existing csv if available
        try:
            df = pd.read_csv(csv_path)
        except (FileNotFoundError, pd.errors.EmptyDataError, csv.Error):
            df = None

        # get the next batch of transactions
        try:
            resp = requests.get(
                f"https://api.zksync.io/api/v0.1/account/{wallet}/history/{n}/{inc}"
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

        # get json response
        j = resp.json()

        # Repeat until response empty
        if not j:
            break

        all_trx = []

        # For every transaction in the batch
        for t in j:

            # clean transaction string
            tx_hash = t["hash"].replace("sync-tx:", "")

            # skip existing transactions to avoid duplicates
            try:
                if df.isin([tx_hash]).any().any():
                    # just for debugging
                    # print(f"Found duplicate: {tx_hash}, skipping!")
                    continue
            except AttributeError:
                pass

            # transaction detail shortcut
            td = t["tx"]

            # init structure for csv
            my_trx = {
                "transactionType": None,
                "date": None,
                "inBuyAmount": None,
                "inBuyAsset": None,
                "outSellAmount": None,
                "outSellAsset": None,
                "feeAsset (optional)": None,
                "feeAmount (optional)": None,
            }

            my_date = t["created_at"]

            # convert to datetime object and then to format needed
            # Accointing needs: ‘MM/DD/YYYY HH:mm:SS’
            dto = datetime.strptime(my_date, "%Y-%m-%dT%H:%M:%S.%f%z")
            my_trx["date"] = dto.strftime("%m/%d/%Y %H:%M:%S")

            # trx coming in
            try:
                if td["to"].lower() == wallet.lower():
                    my_trx["transactionType"] = "deposit"
                    my_trx["inBuyAmount"] = float(td["amount"]) * 1e-18
                    my_trx["inBuyAsset"] = td["token"]
                    my_trx["classification (optional)"] = in_class
                # trx going out
                else:
                    my_trx["transactionType"] = "withdraw"
                    my_trx["outSellAmount"] = float(td["amount"]) * 1e-18
                    my_trx["outSellAsset"] = td["token"]
                    my_trx["classification (optional)"] = out_class

                    my_trx["feeAsset (optional)"] = td["token"]
                    my_trx["feeAmount (optional)"] = float(td["fee"]) * 1e-18
            except KeyError:
                continue

            my_trx["operationId (optional)"] = tx_hash

            all_trx.append(my_trx)

            # count new transactions
            new_c += 1

        # Append to existing df or create new
        if df is None or df.empty:
            df = pd.DataFrame(all_trx)
        else:
            df = df.append(pd.DataFrame(all_trx))

        # Safe df to csv
        df.to_csv(csv_path, index=False)

        # Increment count of processed transactions
        n += inc

        print(f"Processed: {n:9d} Transactions. In csv: {len(df.index):9d}")

    print(f"Done! Added {new_c} new transactions to csv!")


if __name__ == "__main__":
    main()
