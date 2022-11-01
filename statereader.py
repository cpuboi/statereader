#!/usr/bin/env python3


import os
import sys
import gzip
import time
import logging
import argparse

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(stream=sys.stdout,
                    format=Log_Format,
                    level=logging.INFO)
logger = logging.getLogger()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="File to get processed", required=True)
    parser.add_argument("-b", "--bytes", help="Bytestream position, which byte to jump to", required=False, type=int)
    parser.add_argument("-s", "--statefile", help="State file where bytestream is stored", required=False)
    parser.add_argument("-t", "--tail", help="Tail the file continuously", required=False, action='store_true')
    parser.add_argument("-m", "--module", help="external.py with module name external is placed in modules directory",
                        required=False, action='store_true')
    # parser.add_argument("-p", "--processfile", help="File containing python process function.", required=False)
    # parser.add_argument("-n", "--name", help="Name of process function to be imported.", required=False)
    args = parser.parse_args()


    if not args.bytes:
        args.bytes = 0

    return args.file, args.bytes, args.statefile, args.tail, args.module # args.processfile, args.name


class StateReader:
    """
    The StateReader keeps the state of which line from the input file has been processed by the processing function.
    If the processing function crashes, the byte offset of the file is written to a .state file.
    StateReader will pick up from this position and continue processing at the next attempt.

    This is handy if you want to send all lines in a file to an unreliable server that is prone to crashing and you
    dont wish to re-send the data.
    Or for example if you want to keep processing a file that is growing in size like a log file.

    It handles regular text files and gzip files.
    """

    def __init__(self,
                 input_file,
                 processing_function=None,
                 stream_position=0,
                 statefile_path=None,
                 tail=False,
                 module=False):
        self.input_file = input_file
        if not os.path.isfile(self.input_file):
            logger.error(f"{self.input_file} is not a file.")
            exit(1)
        self.line_counter = 0
        self.tail = tail  # Should file get tailed?
        self.file_size = os.stat(self.input_file).st_size
        self.module = module

        """ If this is being used as an object, an alternative processing function is easy to implement """
        if processing_function is None:  # Default to the function that prints each line.
            self.processing_function = self.__print_function
        else:
            self.processing_function = processing_function

        """ If this is being run from the commandline, an external function can get imported from the modules dir."""
        if self.module:
            from modules import external
            self.processing_function = external.external_module

        if statefile_path is None:  #
            self.state_file = self.__statefile_get_filename(input_file, statefile_path)
        else:
            self.state_file = statefile_path

        if stream_position != 0:  # If a custom stream position is chosen, use that.
            self.stream_position = stream_position
        elif os.path.isfile(self.state_file):  # Else if an old state file exists, pick up from the old byte offset.
            self.stream_position = self.__statefile_read(self.state_file)
        else:  # Otherwise, stream position is at the beginning of the file.
            self.stream_position = stream_position

        self.__analyze_file()  # Check if file is Gzip or not, return corresponding generator.

        """
        Start tailing
        """
        if self.tail:
            self.tail_file()

    def __analyze_file(self):
        __gzip_magic = b'\x1f\x8b'
        __test = ''
        if self.input_file.endswith(".gz"):
            with open(self.input_file, 'rb') as __file:
                __test = __file.read(2)
            if __gzip_magic == __test:
                if self.tail:
                    logger.warning("Tailing a gzipped file means the whole file has to be re read everytime.")
                self.generator = self.__gzip_generator(self.input_file, stream_position=self.stream_position)
            else:
                logger.error(f"{self.input_file} is not a real gzip file.")
                sys.exit(1)
        else:
            self.generator = self.__text_generator(self.input_file, stream_position=self.stream_position)

    def __gzip_generator(self, gzip_file, stream_position=None):
        """
        Returns a generator from a gzip file
        Since Python's implementation of gzip is rather slow this might be CPU intensive.
        In order to jump to a stream position the preceeding data needs to be read by the gzip decompression method.
        Gzip needs to read the file from the beginning in order to keep the correct state.
        Therefore jumping to a position might take lots of disk IO.
        """
        __state_file = self.__statefile_get_filename(gzip_file)
        if stream_position is None:
            if os.path.isfile(__state_file):
                stream_position = self.__statefile_read(__state_file)
            else:
                stream_position = 0
        with gzip.open(gzip_file, 'rb') as __gzip_file:
            __gzip_file.seek(stream_position)
            try:
                for line in __gzip_file:
                    stream_position = __gzip_file.tell()  # Record the new stream location
                    yield line, stream_position
            except Exception as e:
                self.statefile_write()
                logger.error(e)

    def __text_generator(self, text_file, stream_position=None):
        """ Returns a generator from a file """
        __state_file = self.__statefile_get_filename(text_file)
        if stream_position is None:
            if os.path.isfile(__state_file):
                stream_position = self.__statefile_read(__state_file)
            else:
                stream_position = 0
        with open(text_file, 'rb') as __text_file:
            __text_file.seek(stream_position)
            try:
                for line in __text_file:
                    stream_position = __text_file.tell()  # Record the new stream location
                    yield line, stream_position
            except Exception as e:
                self.statefile_write()
                logger.error(e)

    def __statefile_get_filename(self, original_file, statefile_path=None):
        def __has_write_permission(__test_file):
            if os.path.isfile(__test_file):
                return os.access(__test_file, os.W_OK)
            else:
                return os.access(os.path.dirname(__test_file), os.W_OK)

        if not statefile_path:
            __statefile_path = os.path.abspath(original_file) + ".state"
        else:
            __statefile_path = statefile_path
        if __has_write_permission(__statefile_path):
            return __statefile_path
        else:
            logger.error(f"Can not write to state file {__statefile_path}")
            sys.exit(1)

    def __statefile_read(self, state_file_path):
        try:
            with open(state_file_path, 'r') as __state_file:
                return int(__state_file.read())
        except Exception as e:
            logger.error(e)

    def __run_process(self, input_file, processing_function, stream_position=None):
        """
        Starts generator, if state file exists, start from location in state file.
        If error occurs, write latest location to state file.
        """
        count = 0
        generator_object = self.__gzip_generator(input_file, stream_position=stream_position)
        for item in generator_object:
            try:
                count += 1
                processing_function(item)
                if count > 30:
                    generator_object.throw(Exception, "Error")
            except StopIteration:
                print("Done processing stuff.")

    def __print_function(self, in_string):
        if isinstance(in_string, __builtins__.bytes):
            print(in_string.decode())
        else:
            print(in_string)

    def process_one_line(self):
        try:
            line, stream_position = next(self.generator)
            self.stream_position = stream_position
            self.processing_function(line)
            return True
        except:
            return False

    def run_processing_function(self, limit_lines=False):
        for line, stream_position in self.generator:
            try:
                self.stream_position = stream_position
                self.processing_function(line)
                self.line_counter += 1
                if self.line_counter % 100_000 == 0:
                    logger.info(f"Processed {self.line_counter} lines.")
                if limit_lines and self.line_counter > limit_lines:
                    self.line_counter = 0
                    break
            except KeyboardInterrupt:
                self.statefile_write()
                logger.info("CTRL+C")
                sys.exit(1)
            except Exception as e:
                logger.error(e)
                self.statefile_write()

    def tail_file(self):
        old_size = 0
        sleep_period = 5
        while True:
            try:
                self.file_size = os.stat(self.input_file).st_size  # Update file size
                if self.file_size > old_size:  # If file size has increased, re run the process.
                    logger.debug(f"{self.file_size} is larger than {old_size}, processing new data.")
                    self.run_processing_function()
                    self.__analyze_file()  # Create a new generator
                    old_size = self.file_size  # Save file size to temporary variable.
                    self.statefile_write()  # Update the statefile
                    time.sleep(1)
                else:
                    logger.info(f"{self.file_size} is the same as old size: {old_size}, sleeping {sleep_period} seconds")
                    time.sleep(sleep_period)
            except KeyboardInterrupt:
                self.statefile_write()
                logger.info("CTRL+C")
                exit(1)


    def statefile_write(self):
        """Update the state file with the last working byte position."""
        try:
            with open(self.state_file, 'w') as __state_file:
                __state_file.write(str(self.stream_position))
                return True
        except Exception as e:
            logger.error(e)
            return False

if __name__ == "__main__":
    input_file, bytes, statefile, tail, module, = parse_arguments()
    sr = StateReader(input_file=input_file, stream_position=bytes, statefile_path=statefile, tail=tail, module=module)
    logger.info(f"Processing {input_file}")
    sr.run_processing_function()
