from os import environ, remove
from ftplib import FTP_TLS
from pathlib import Path
import time 
import schedule
import sys
import pandas as pd
import json

""" Extracting data from web to FTP
 by yourstrulyhb 

Reference:
- https://docs.python.org/3/library/ftplib.html
- https://pandas.pydata.org/docs/dev/reference/api/pandas.read_csv.html
- https://docs.python.org/3/library/pathlib.html
- https://schedule.readthedocs.io/en/stable/examples.html
   """


def get_ftp() -> FTP_TLS:
   """ Start an FTP server.

   Return an FTP server [object].
   """
   # Get FTP details
   FTPHOST = environ.get("FTPHOST")
   FTPUSER = environ.get("FTPUSER")
   FTPPASS = environ.get("FTPPASS")
   FTPPORT = environ.get("FTPPORT")

   print("Creating FTP server...")

   # Create instance of FTP class
   ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
   # Set up secure data connection.  
   ftp.prot_p()                           

   print("FTP server running...")

   return ftp
 

def upload_to_ftp(ftp: FTP_TLS, file_source: Path):
   """ Upload a file to FTP server.

   :param ftp: FTP server
   :param file_source: A Path object containing path of file to upload
   """
   with open(file_source, "rb") as fp:
      ftp.storbinary(f"STOR {file_source.name}", fp)


def delete_file(file_source: str | Path):
   remove(file_source)


def read_csv(config: dict) -> pd.DataFrame:
   """ Reads data in a CSV file and inputs data into a dataframe.

      :param config: a dictionary containing name-value pairs for parameters in creating a DataFrame

      Returns equivalent DataFrame of an CSV file.
   """

   url = config["URL"]
   params = config["PARAMS"]

   return pd.read_csv(url, **params)


def web_to_ftp_pipeline():
  # Load source configuration
   with open("ofac_cons_config.json", "rb") as fp:
      config = json.load(fp)

   # Set up an authenticated FTP object
   ftp = get_ftp()

   # Download files
   for source_name, source_config in config.items():
      file_name =  Path(source_name + ".csv")
      df = read_csv(source_config)

      df.to_csv(file_name, index=False)
      print(f"Downloaded {file_name}.")

      upload_to_ftp(ftp, file_name)
      print(f"Uploaded {file_name} to FTP server.")

      # Removed local CSV files
      delete_file(file_name)
      print(f"Deleted {file_name}.")
   



if __name__=="__main__":

   # Run manually
   param = sys.argv[1]

   if param.lower() == "manual":
      web_to_ftp_pipeline()

   elif param.lower() == "schedule":
       # Schedule extraction
      schedule.every().day.at("15:25").do(web_to_ftp_pipeline)

      while True:
         schedule.run_pending()
         time.sleep(1)

   else: 
      print("Invalid parameter.")

