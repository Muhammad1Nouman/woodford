import logging

logging.basicConfig(
    filename=r"C:\Users\mnouman\Desktop\Project_WoodFordBros\runlog.txt",
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

from ingest_to_db import ingestion
from transformation import transformation
from pbi_data import extract_all_data
from crew_weeks import update_crew_weeks

def execution():
    logging.info("Starting execution")
    try:
        ingestion()
        transformation()
        extract_all_data()
        update_crew_weeks()
        logging.info("Execution finished successfully")
    except Exception as e:
        logging.error(f"Execution failed: {e}")

if __name__ == "__main__":
    execution()
