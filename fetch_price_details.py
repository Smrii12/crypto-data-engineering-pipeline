import json
import urllib.request
import ssl
import os
from datetime import datetime

print("Executing pipline Engine")

url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"

headers={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64) AppleWebKit/537.36 (KHTML , like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

context=ssl._create_unverified_context()

def  validate_crypto_record(raw_token):
    token_id=raw_token.get('id')
    current_price=raw_token.get('current_price')
    market_cap=raw_token.get('market_cap')
    total_volume=raw_token.get('total_volume')


    if None in (token_id,current_price,market_cap,total_volume):
        raise ValueError("Missing critical financial indicators.")

    return {
        "token_id":str(token_id),
        "usd_price":float(current_price),
        "usd_market_cap":float(market_cap),
        "usd_24h_volume":float(total_volume)
    }

try:
    req=urllib.request.Request(url,headers=headers)
    with urllib.request.urlopen(req,timeout=10) as response:
        raw_text=response.read().decode('utf-8')

        raw_api_list=json.loads(raw_text)

        # #--------------------------Intentional------------------------
        # if len(raw_api_list)>1:
        #     print("\n Injecting a broken text token into the second asset row for testing")
        #     raw_api_list[1]['market_cap']="CORRUPTED_TEXT_STRING"
        # #--------------------------Intentional------------------------
        production_sink=[]
        dead_letter_queue=[]
        for entry in raw_api_list:
            name_slug=entry.get('id','unknown')

            try:
                clean_data=validate_crypto_record(entry)
                production_sink.append(clean_data)
            except Exception as error_msg:
                quarentine={
                    "token_id": name_slug,
                    "raw_payload":entry,
                    "reason":str(error_msg)

                }
                dead_letter_queue.append(quarentine)
        today_date=datetime.now().strftime("%Y-%m-%d")
        time_slug=datetime.now().strftime("%H%M%S")

        production_folder=f"data_lake/production/date={today_date}"
        dlq_folder=f"data_lake/dlq/date={today_date}"

        os.makedirs(production_folder,exist_ok=True)
        os.makedirs(dlq_folder,exist_ok=True)

        if production_sink:
            prod_file_path=f"{production_folder}/batch_{time_slug}.json"
            with open(prod_file_path,'w') as prod_file:
                json.dump(production_sink,prod_file,indent=4)
            print(f"Saved Clean Data: {prod_file_path}")

        if dead_letter_queue:
            dlq_file_path=f"{dlq_folder}/poisoned_{time_slug}.json"
            with open(dlq_file_path,'w') as dlq_file:
                json.dump(dead_letter_queue,dlq_file,indent=4)
            print(f"Saved Anomalies to Quarantined DLQ: {dlq_file_path}")
        else:
            print("DLQ Status: Clean, No anomalies to save during this window.")

        print("Pipline completed!")

except Exception as error:
    print("Something went wrong: ",error)