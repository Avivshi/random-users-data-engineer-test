import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists


class RandomUsers:

    def __init__(self):
        details = pd.read_json('connection_details.json').iloc[0]
        db_url = f'mysql+mysqlconnector://{details.user}:{details.password}@{details.host}:{details.port}/{details.database}'
        if not database_exists(db_url):
            create_database(db_url)
        self._engine = create_engine(db_url)

        api_url = 'https://randomuser.me/api/?results=4500'
        data = requests.get(api_url).json()
        self._users = pd.json_normalize(data['results'])

    def split_by_gender(self):
        self.store_in_db(self._users[self._users.gender == 'male'], 'aviv_shimoni_test_male')
        self.store_in_db(self._users[self._users.gender == 'female'], 'aviv_shimoni_test_female')

    def split_to_subsets(self):
        subsets = []
        minimum = 10
        maximum = 20

        while maximum <= 110:
            subset = self._users[(self._users['dob.age'] >= minimum) & (self._users['dob.age'] < maximum)]
            subsets.append(subset)
            minimum = maximum
            maximum += 10

        for index, subset in enumerate(subsets):
            self.store_in_db(subset, f'aviv_shimoni_test_{index + 1}')

    def top_20_last_registered_users(self):
        query = '''SELECT * FROM aviv_shimoni_test_male 
        UNION 
        SELECT * FROM  aviv_shimoni_test_female ORDER BY `registered.date` DESC LIMIT 20'''
        top_20 = pd.read_sql(query, self._engine)
        self.store_in_db(top_20, 'aviv_shimoni_test_20')

    def union_tables_5_and_20(self):
        self.union_tables_to_json('aviv_shimoni_test_20', 'aviv_shimoni_test_5', 'UNION', 'first.json')

    def unionall_tables_2_and_20(self):
        self.union_tables_to_json('aviv_shimoni_test_20', 'aviv_shimoni_test_2', 'UNION ALL', 'second.json')

    def union_tables_to_json(self, table1, table2, union_type, file_name):
        query = "SELECT * FROM {} {} SELECT * FROM {}".format
        union_dataset = pd.read_sql(query(table1, union_type, table2), self._engine)
        union_dataset.to_json(file_name)

    def store_in_db(self, df, table_name):
        self._engine.execute(f"DROP TABLE IF EXISTS {table_name}")
        df.to_sql(con=self._engine, name=table_name, index=False)

    def main(self):
        self.split_by_gender()
        self.split_to_subsets()
        self.top_20_last_registered_users()
        self.union_tables_5_and_20()
        self.unionall_tables_2_and_20()


if __name__ == "__main__":
    RandomUsers().main()
