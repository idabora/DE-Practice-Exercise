import json
import os

import pandas as pd


def main():
    data_folder = os.path.join(os.getcwd(), 'data')
    csv_file_path = os.path.join(os.getcwd(), 'csvfiles/')
    print(csv_file_path)

    for dirpath, dirname, filenames in os.walk(data_folder):
        for file in filenames:
            if file.endswith('.json'):

                file_path = os.path.join(dirpath, file)
                with open(file_path, 'r') as json_file:

                    data = json.load(json_file)
                    df = pd.json_normalize(data)

                    if 'geolocation.coordinates' in df.columns:
                        coords_df = pd.DataFrame(df['geolocation.coordinates'].tolist(),columns=['geolocation.coordinates0', 'geolocation.coordinates1'])
                        df = pd.concat([df.drop(columns=['geolocation.coordinates']), coords_df], axis=1)

                csv_file_name = file.split('.json')[0]
                df.to_csv(csv_file_path+csv_file_name+'.csv', index=False)
                    

if __name__ == "__main__":
    os.makedirs('csvfiles', exist_ok=True)
    main()
