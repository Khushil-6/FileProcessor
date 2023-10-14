# app.py
import time
from FileProcessor.processor import Processor

if __name__ == "__main__":
    start_time = time.time()
    processor = Processor(n=10)
    processor.process_files()
    end_time = time.time()  # Record the end time
    processing_time = end_time - start_time  # Calculate the processing time
    print(f"Processing completed in {processing_time:.2f} seconds.")
