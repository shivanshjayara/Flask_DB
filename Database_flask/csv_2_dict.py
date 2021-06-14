import csv


class csv_dict:

    def convert_to_dict(self):
        with open("customer_db.csv") as read_file:
            read_data = csv.DictReader(read_file)
            stored_list = []

            for i in read_data:
                stored_list.append(dict(i))
        return stored_list
