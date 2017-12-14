import pandas as pd
import sys

if __name__ == "__main__":
    df = pd.read_csv('input.csv', parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    subset_1 = df[(df.VendorID == 1) | (df.VendorID == 2)]
    subset_2 = subset_1[(subset_1.total_amount > 0) & (subset_1.total_amount > subset_1.fare_amount) \
                        & (subset_1.total_amount > subset_1.tip_amount) \
                        & (subset_1.total_amount > subset_1.improvement_surcharge) \
                        & (subset_1.total_amount > subset_1.mta_tax) \
                        & (subset_1.total_amount > subset_1.extra) \
                        & (subset_1.total_amount > subset_1.tolls_amount)]
    subset_3 = subset_2[(subset_2.extra >= 0) & (subset_2.trip_distance > 0) \
                        & (subset_2.tip_amount >= 0) & (subset_2.fare_amount > 0) \
                        & (subset_2.tolls_amount >= 0) & (subset_2.mta_tax >= 0.50)]
    subset_4 = subset_3[(subset_3.pickup_latitude >= 40.5774) & (subset_3.pickup_latitude <= 40.9176) \
                        &(subset_3.dropoff_longitude >= -74.15) & (subset_3.dropoff_longitude <= -73.7004) \
                        &(subset_3.dropoff_latitude >= 40.5774) & (subset_3.dropoff_latitude <= 40.9176) \
                        &(subset_3.pickup_longitude >= -74.15) & (subset_3.pickup_longitude <= -73.7004)]
    # 2016-07 to 2016-12
    #subset_4 = subset_3[(subset_3.PULocationID >= 1) & (subset_3.PULocationID <= 263) \
    #                    & (subset_3.DOLocationID >= 1) & (subset_3.DOLocationID <= 263)]
    subset_5 = subset_4[(subset_4.passenger_count >= 1) & (subset_4.passenger_count <= 9)]
    subset_6 = subset_5[(subset_5.RatecodeID >= 1) & (subset_5.RatecodeID <= 6)]
    # 2015 1-6
    # subset_6 = subset_5[(subset_5.RateCodeID >= 1) & (subset_5.RateCodeID <= 6)]
    subset_7 = subset_6[(subset_6.payment_type >= 1) & (subset_6.payment_type <= 6)]
    # The year and month should be the current year and month in the csv file
    subset_8 = subset_7[(subset_7.tpep_pickup_datetime.dt.year == 2016) \
                        & (subset_7.tpep_pickup_datetime.dt.month == 7)]
    subset_9 = subset_8[(subset_8.tpep_dropoff_datetime.dt.year == 2016) \
                        & (subset_8.tpep_dropoff_datetime.dt.month == 7)]
    subset_10 = subset_9[(subset_9.improvement_surcharge > 0.299) & (subset_9.improvement_surcharge < 0.301)]
    subset_11 = subset_10[(subset_10.store_and_fwd_flag == 'Y') | (subset_10.store_and_fwd_flag == 'N')]
    subset_11.to_csv('output.csv', index=False)