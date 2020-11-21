import pandas as pd


class Conversion:

    def conversion_rate(self, sql, engine):
        df = pd.read_sql(sql=sql, con=engine)
        print("conversion rate is ", df.to_string(index=False,header=False))
