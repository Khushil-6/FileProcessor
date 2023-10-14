# processor.py
from concurrent.futures import ThreadPoolExecutor
from FileProcessor.config import S3_BUCKET
from FileProcessor.database import MongoDB
from pyspark.sql import SparkSession
import subprocess, logging, os, shutil
import pefile as pefile


class Processor:
    def __init__(self, n, file_paths=None):
        self.n = n
        self.spark = SparkSession.builder.appName("FileProcessor").getOrCreate()
        self.mongodb = MongoDB()
        self.file_paths = file_paths
        self.temp_dir = "Staged_Files"
        self._setup_logger()

    def process_files(self):
        # Fetch n/2 malware and n/2 clean files from S3
        s3_file_paths = self._fetch_s3_files() if self.file_paths is None else self.file_paths
        self.logger.info("Starting to process files !!!")

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor() as executor:
            executor.map(self._process_and_store_metadata, s3_file_paths)

        # Clean up temporary files
        self._clean_temporary_files()

    def _fetch_s3_files_list(self):
        # Use ThreadPoolExecutor to run AWS CLI commands concurrently
        with ThreadPoolExecutor() as executor:
            clean_files_future = executor.submit(self._list_s3_objects, "0")
            malware_files_future = executor.submit(self._list_s3_objects, "1")

        clean_files = clean_files_future.result()
        malware_files = malware_files_future.result()

        # Combine malware and clean files from both catalogs and limit the number of files to n
        s3_file_paths = clean_files[:self.n // 2] + malware_files[:self.n // 2]

        return s3_file_paths

    def _list_s3_objects(self, catalog):
        try:
            command = ["aws", "s3", "--no-sign-request", "ls", f"s3://{S3_BUCKET}/{catalog}/"]
            output = subprocess.check_output(command, universal_newlines=True)
            return [f"{catalog}/" + line.split()[-1] for line in output.splitlines()]
        except Exception as e:
            self.logger.error("Failed to connect to Endpoint", {str(e)})

    def _fetch_s3_files(self):
        # Create a temporary directory to store downloaded files
        os.makedirs(self.temp_dir, exist_ok=True)

        downloaded_files = []

        with ThreadPoolExecutor() as executor:
            futures = []
            try:
                for s3_file_path in self._fetch_s3_files_list():
                    # Extract the file name from the S3 path
                    file_name = os.path.basename(s3_file_path)
                    local_file_path = os.path.join(self.temp_dir, file_name)
                    actual_file_path = f"s3://{S3_BUCKET}/{s3_file_path[:1]}/{s3_file_path[2:]}"

                    # Use the AWS CLI to copy the file from S3 to the local directory
                    copy_command = ["aws", "s3", "--no-sign-request", "cp", actual_file_path, local_file_path]
                    future = executor.submit(subprocess.run, copy_command)
                    futures.append((future, local_file_path, actual_file_path))

                for future, local_path, actual_path in futures:
                    future.result()  # Wait for the copy to complete
                    downloaded_files.append((local_path, actual_path))

            except Exception:
                self.logger.error(f"Failed to download - s3://{S3_BUCKET}/{s3_file_path[:1]}/{s3_file_path[2:]}")

        return downloaded_files

    def _process_and_store_metadata(self, file_path):
        try:
            local_path, actual_path = file_path[0], file_path[1]
            if not self.mongodb.metadata_exists(actual_path):
                metadata = self._preprocess_file(local_path, actual_path)
                self.mongodb.store_metadata(metadata)
                self.logger.info(f"Processed and stored metadata for {actual_path}")
            else:
                self.logger.info(f"Metadata already exists for {actual_path}, skipping")

        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")

    def _preprocess_file(self, local_path, actual_path):
        try:
            # Read the file using PySpark
            file_rdd = self.spark.read.text(local_path)
            file_type = self._get_file_type(local_path)
            architecture = self._get_architecture(local_path)
            imports, exports = self._get_imports_and_exports(local_path)
            # Calculate file size based on the RDD count
            file_size = file_rdd.count()

            metadata = {
                "file_path": actual_path,
                "size": file_size,
                "file_type": file_type,
                "architecture": architecture,
                "imports": imports,
                "exports": exports
            }

            return metadata
        except Exception as e:
            self.logger.error(f"Error processing file {local_path}: {str(e)}")
            return None

    def _setup_logger(self):
        self.logger = logging.getLogger("FileProcessor")
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    @staticmethod
    def _get_imports_and_exports(file_path):
        try:
            pe = pefile.PE(file_path)
            imports = len(pe.DIRECTORY_ENTRY_IMPORT) if hasattr(pe, 'DIRECTORY_ENTRY_IMPORT') else 0
            exports = len(pe.DIRECTORY_ENTRY_EXPORT.symbols) if hasattr(pe, 'DIRECTORY_ENTRY_EXPORT') else 0
            return imports, exports
        except Exception as e:
            return 0, 0

    @staticmethod
    def _get_architecture(file_path):
        try:
            pe = pefile.PE(file_path)
            if pe.FILE_HEADER.Machine == 0x14C:
                return "x32"
            elif pe.FILE_HEADER.Machine == 0x8664:
                return "x64"
            else:
                return "unknown"
        except Exception as e:
            return "unknown"

    @staticmethod
    def _get_file_type(file_path):
        try:
            pe = pefile.PE(file_path)
            if pe.is_dll():
                return "dll"
            elif pe.is_exe():
                return "exe"
            else:
                return "unknown"
        except Exception as e:
            return "unknown"

    def remove_file(self, file_path):
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            self.logger.error(f"Error removing {file_path}: {str(e)}")

    def _clean_temporary_files(self):
        if not os.path.exists(self.temp_dir):
            return

        file_list = [os.path.join(self.temp_dir, file_name) for file_name in os.listdir(self.temp_dir)]

        # Create a pool of worker processes
        with ThreadPoolExecutor() as executor:
            executor.map(self.remove_file, file_list)


