from io import StringIO
from mooda import WaterFrame
import pandas as pd
import csv
import re
import numpy as np


SN_REGEX = re.compile(r'(?:LGR S/N: |Serial Number:)(\d+)')


class Hobo:

    """Class to import Hobo data from a csv file"""

    def __init__(self, path):
        """
        It creates the instance variable path

        Parameters
        ----------
            path: str
                Path of the csv file.
        """
        self.path = path

    def from_csv(self, qc_tests=False):
        """
        It opens a csv file that contains data from an HOBO instrument.

        Parameters
        ----------
            model: str
                Model of the instrument.

            qc_tests: bool (optional)
                It indicates if QC test should be passed.

        Returns
        -------
            wf: WaterFrame """

        # open file
        self.f = open(self.path, mode='rt', encoding='utf-8-sig')

        # declare metadata variables
        self.title, self.sn = None, None

        # declare index variables
        self.timestamp = None

        # declare dict to save name, long_name and units
        self.name = {'temp': [None, None, None], 'pres': [None, None, None],
                     'rh': [None, None, None], 'batt': [None, None, None]}

        def find_units(header):
            header_units = header.split(",", 2)[1].strip()
            units = header_units.split(" ")[0].strip()
            return units

        def find_name(header):
            header_name = header.rsplit(",", 1)[1].strip(" )")
            if header_name.split(":")[0].strip() == "LBL":
                name = header_name.split(":")[1].strip()
            else:
                name = header.split(",", 1)[0].strip()
            return name

        def find_long_name(header):
            long_name = header.split(",", 1)[0].strip()
            return long_name

        def find_col_timestamp(headers):
            for i, header in enumerate(headers):
                if 'Date Time' in header or 'Fecha Tiempo' in header:
                    return i

        def find_col_temperature(headers):
            for i, header in enumerate(headers):
                if 'High Res. Temp.' in header or 'High-Res Temp' in header:
                    self.name['temp'] = [find_name(header), find_long_name(header),
                                         find_units(header)]
                    # self.temp_units = find_units(header)
                    # self.temp = find_name(header)
                    # return 'TEMP'
            for i, header in enumerate(headers):
                for s in ('Temp,', 'Temp.', 'Temperature'):
                    if s in header:
                        self.name['temp'] = [find_name(header), find_long_name(header),
                                             find_units(header)]
                        # self.temp_units = find_units(header)
                        # self.temp = find_name(header)
                        # return 'TEMP'

        def find_col_preassure(headers):
            for i, header in enumerate(headers):
                if 'Pres abs,' in header:
                    self.name['pres'] = [find_name(header), find_long_name(header),
                                         find_units(header)]
                    # self.pres = find_units(header)
                    # self.pres = find_name(header)
                    # return 'PRES'

        def find_col_rh(headers):
            for i, header in enumerate(headers):
                if 'RH,' in header:
                    self.name['rh'] = [find_name(header), find_long_name(header),
                                       find_units(header)]
                    # return 'RH'

        def find_col_battery(headers):
            for i, header in enumerate(headers):
                if 'Batt, V' in header:
                    self.name['batt'] = [find_name(header), find_long_name(header),
                                         find_units(header)]
                    # return 'BATT'

        def find_columns(header):
            """ Find and set column names for headers """
            headers = next(csv.reader(StringIO(header)))
            self.headers = headers
            self.timestamp = find_col_timestamp(headers)
            find_col_temperature(headers)
            find_col_preassure(headers)
            find_col_rh(headers)
            find_col_battery(headers)

        def find_headers():
            while self.timestamp is None:
                header = next(self.f)
                if self.title is None:
                    self.title = header.strip().split(":")
                if self.sn is None:
                    sn_match = SN_REGEX.search(header)
                    self.sn = sn_match.groups()[0] if sn_match else None
                find_columns(header)
            return header

        # find headers
        find_headers()

        # Creation of a WaterFrame
        wf = WaterFrame()
        metadata = {}
        metadata[self.title[0]] = self.title[1].strip()
        metadata["S/N"] = self.sn.strip()

        # Load metadata to Waterframe
        wf.metadata = metadata

        # Create dataframe from csv
        df = pd.read_csv(self.f,
                         names=self.headers,
                         index_col=self.timestamp)

        df = df.replace(np.nan, '', regex=True)

        # Set index to datetime
        df.index = pd.to_datetime(df.index)
        df.set_index(pd.DatetimeIndex(df.index, inplace=True))

        # Rename index
        df.index.name = 'Time'

        # Rename columns
        for col in df.columns:
            if 'Temp' in col:
                df.rename(columns={col: self.name['temp'][0]}, inplace=True)
            elif 'Pres' in col:
                df.rename(columns={col: self.name['pres'][0]}, inplace=True)
            elif 'Batt' in col:
                df.rename(columns={col: self.name['batt'][0]}, inplace=True)
            elif 'RH' in col:
                df.rename(columns={col: self.name['rh'][0]}, inplace=True)

        # Filter columns only if they are present
        df = df.filter(items=[self.name['temp'][0], self.name['pres'][0], self.name['batt'][0],
                              self.name['rh'][0]])

        # Add QC keys
        for key in df.keys():
            df["{}_QC".format(key)] = 0

        # Add DataFrame into the WaterFrame
        wf.data = df.copy()

        # Change parameter names and add QC columns
        for parameter in wf.parameters():
            for key, value in self.name.items():
                if value[0] == parameter:
                    wf.meaning[parameter] = {"long_name": value[1], "units": value[2]}

        # Creation of QC Flags following OceanSites recomendation
        if qc_tests:
            for parameter in wf.parameters():
                # Reset QC Flags to 0
                wf.reset_flag(key=parameter, flag=0)
                # Flat test
                wf.flat_test(key=parameter, window=0, flag=4)
                # Spike test
                wf.spike_test(key=parameter, window=0, threshold=3, flag=4)
                # Range test
                wf.range_test(key=parameter, flag=4)
                # Change flags from 0 to 1
                wf.flag2flag(key=parameter, original_flag=0,
                             translated_flag=1)
        print(wf.data)
        print(wf)
        return wf