# File Processor

The File Processor is a Python application for processing and analyzing files, with a primary focus on and metadata extraction. This application is designed to work with files stored in an Amazon S3 bucket.

## Prerequisites

Before using the File Processor, ensure you have the following prerequisites installed:

- [Python](https://www.python.org/): The application is written in Python and requires Python to run.
- [AWS CLI](https://aws.amazon.com/cli/): The AWS Command Line Interface is used to interact with Amazon S3 for file retrieval.
- [PySpark](https://spark.apache.org/docs/latest/api/python/): PySpark is used for reading and processing the contents of the files.
- [PEfile](https://github.com/erocarrera/pefile): PEfile is used for parsing PE (Portable Executable) files.

## Installation

1. Clone the project repository:

   ```bash
   git clone https://github.com/yourusername/your-repo.git
   ```

2. Install the required Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

The File Processor application provides several features:

1. **File Processing**: This application is designed to process a specific number (`n`) of malware and clean files. It retrieves these files from an S3 bucket, processes them, and stores metadata in a database.

2. **Metadata Storage**: Metadata for each processed file, including file size, type, architecture, imports, and exports, is stored in a MongoDB database.

3. **Cleanup**: Temporary files used during processing are removed automatically to free up space.

To use the application, you can create an instance of the `Processor` class with the desired `n` value and call the `process_files` method.

```python
from FileProcessor.processor import Processor

# Create a Processor instance with the desired 'n' value
processor = Processor(n=100)

# Process files and store metadata
processor.process_files()
```

## Configuration

The configuration for the application can be customized by editing the `config.py` file. Make sure to set the appropriate S3 bucket name and other configurations in this file.
