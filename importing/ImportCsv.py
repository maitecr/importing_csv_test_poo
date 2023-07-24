import pandas as pd
import time

class ImportCsv:
    def import_csv(self, file_path):
        start_time = time.time()

        self.data_file = pd.read_csv(file_path,
                                encoding="ISO-8859-1",
                                quotechar='"',
                                delimiter=';',
                                low_memory=False
                                )
        self.data_comma_to_dot = self.data_file.stack().str.replace(',', '.').unstack()
        # Filling NaN values with zero
        self.data_final = self.data_comma_to_dot.fillna(value=0)

        print("Importing time:")
        print("--- %s seconds ---" % (time.time() - start_time))
        return self.data_final
