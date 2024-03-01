import collections
import datetime
import aiofiles
import logging
import os
import io
import shutil
import hashlib
import uuid
import asyncio
import json

from flask import Flask, request, jsonify, send_file, make_response

# import reedsolo


class NullException(Exception):
    """Exception raised for null values"""


class NullKeyValueException(NullException):
    """Exception raised for null key or value."""


class NotFoundException(Exception):
    """Exception raised when an item is not found."""


class BucketNotFoundException(NotFoundException):
    """Exception raised when an item is not found."""


class ObjectAlreadyExistsException(Exception):
    """Exception raised when an item is not found."""


# class ReedSolomonEncoder:
#     def __init__(self, data_shards, parity_shards):
#         """
#         Implements the Reed Solomon Codec
#
#         :param data_shards:
#         :param parity_shards:
#         """
#         self.data_shards = data_shards
#         self.parity_shards = parity_shards
#         self.encoder = reedsolo.RSCodec(self.parity_shards)
#
#     def encode(self, data):
#         """
#         :param data:
#         :return encoded_data:
#         """
#         encoded_data = self.encoder.encode(data)
#         return encoded_data
#
#     def decode(self, encoded_data):
#         """
#         :param encoded_data:
#         :return decoded data:
#         """
#         decoded_data = self.encoder.decode(encoded_data)
#         return b''.join(decoded_data)


class MetaData:
    """
    Manages the meta data for a bucket.
    It stores key-value pairs representing metadata entries.
    """

    def __init__(self) -> None:
        self.__meta_data = {}

    def add_meta_data(self, key, value):
        """Add a metadata entry."""
        if key is not None and value is not None:
            self.__meta_data[key] = value
        else:
            raise NullKeyValueException()

    def add_all_meta_data(self, meta_dict: dict):
        """Add multiple metadata entries."""
        if len(meta_dict) != 0:
            self.__meta_data.update(meta_dict)

    def get_meta_data(self, key):
        """Retrieve a metadata entry."""
        if key is not None:
            if key in self.__meta_data.keys():
                return self.__meta_data[key]
            else:
                raise NotFoundException(f"Metadata with key '{key}' not found")
        else:
            raise NullKeyValueException("Key cannot be None")

    def update_meta_data(self, key, value):
        """
        Update a metadata entry.

        :param  key:
        :param  value:
        :return None:
        :raise  NotFoundException and NullKeyValueException:
        """
        if key is not None:
            if key in self.__meta_data.keys():
                self.__meta_data[key] = value
            else:
                raise NotFoundException(f"Metadata with key '{key}' not found")
        else:
            raise NullKeyValueException("Key cannot be None")

    def delete_meta_data(self, key):
        """
        Deletes a metadata entry.
        :param  key:
        :return None:
        :raise  NotFoundException and NullKeyValueException:
        """
        if key is not None:
            if key in self.__meta_data.keys():
                del self.__meta_data[key]
            else:
                raise NotFoundException(f"Metadata with key '{key}' not found")
        else:
            raise NullKeyValueException("Key cannot be None")

    def get_all_meta_data(self):
        """
        Retrieve all metadata entries.

        :return dict:
        """
        return self.__meta_data


class FileLogger:
    """
    Handles logging operations to a file.
    """

    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self._configure_logging()

    def _configure_logging(self):
        """
        Logging configuration

        :return None:
        """
        logging.basicConfig(
            filename=self.log_file_path,
            level=logging.INFO,
            format="[%(asctime)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    def log(self, data):
        """
        A file based WAL logging utility

        :param  data:
        :return None:
        """
        """Logs the provided data."""
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{current_time}] {data}"
        logging.info(log_entry)


class ConsistentNode:
    """
    Represents a node in consistent hashing.

    Attributes:
        node (str): The identifier for the node.
        is_down (bool): Indicates whether the node is currently running or not.

    Methods:
        __init__(self, _node): Initializes a ConsistentNode instance.
        __str__(self): Returns a string representation of the ConsistentNode.
    """

    def __init__(self, _node):
        self.node = _node
        self.is_down = False

    def __str__(self):
        return f'{self.node} {self.is_down}'


class ConsistentHashing:
    """
    `Will be used for managing the load distribution to the optimal`

        Implements consistent hashing with virtual nodes.

        Attributes:
            __ring (ConsistentHashing.Ring): The ring structure to store nodes.

        Methods:
            __init__(self, capacity): Initializes a ConsistentHashing instance with a specified capacity.
            put(self, node): Adds a node to the consistent hashing ring.
            get_request_server(self, request): Returns the node responsible for handling a given request.
            get_ring(self): Returns the underlying ring structure.
            soft_delete(self, node): Marks a node as down in a soft delete fashion.
    """

    class Hash:
        """
                Provides hashing functionality for consistent hashing.

                Methods:
                    find_index(_val, final_capacity): Calculates the hash index for a given value.
        """

        @staticmethod
        def find_index(_val, final_capacity: int):
            if _val is None:
                raise ValueError("None types are forbidden")
            return int(hashlib.md5(str(_val).encode()).hexdigest(), 16) % final_capacity

    class Ring:
        """
                Represents the ring structure in consistent hashing.

                Attributes:
                    __ring (list): The array representing the ring.
                    capacity (int): The current capacity of the ring.

                Methods:
                    __init__(self, capacity): Initializes a Ring instance with a specified capacity.
                    __str__(self): Returns a string representation of the Ring.
                    get_ring(self): Returns the underlying array representing the ring.
                    put_server(self, _node): Adds a server node to the ring.
                    should_expand(self): Checks if the ring needs expansion.
                    expand_ring(self): Expands the ring if needed.
                    get_server(self, request): Returns the server node responsible for handling a given request.
                    update(self, _node, n_node): Updates a node in the ring.
                    delete(self, _node): Deletes a node from the ring.
                    soft_delete(self, _node): Marks a node as down in a soft delete fashion.
        """

        def __init__(self, capacity: int = 1_000):
            self.__ring = [None] * capacity
            self.capacity = capacity

        def __str__(self):
            return f'{self.__ring}'

        def get_ring(self):
            """
            Give the current ring instance

            :return Current ring instance:
            """
            return self.__ring

        def put_server(self, _node):
            """
            Put the data in the following way
            find index if node at index is None
            then it will put the data else will find
            the empty spot to fill it.

            :param _node:
            :return None:
            """
            if _node is not None:
                c_index = ConsistentHashing.Hash.find_index(_node, len(self.__ring))
                if self.__ring[c_index] is None:
                    self.__ring[c_index] = _node
                else:
                    c_idx = c_index
                    for i in range(c_index, len(self.__ring)):
                        if self.__ring[i] is None:
                            self.__ring[i] = _node
                            # c_idx += 1
                            break
                        c_idx += 1

                    if c_idx == len(self.__ring):
                        for i in range(0, c_index):
                            if c_index == i + 1 and self.__ring[i] is not None:
                                self.__ring.append(_node)
                                self.expand_ring()

        def should_expand(self):
            """
            Check whether the ring should expand or not

            :return bool:
            """
            for val in self.__ring:
                if val is None:
                    return False
            return True

        def expand_ring(self):
            """
            Implements the expansion of ring and
            rearrangement of the nodes based on
            the new capacity.

            :return None:
            """
            if self.should_expand():
                n_capacity = self.capacity * 2 + 1
                n_ring = [None] * n_capacity
                for val in self.__ring:
                    idx = ConsistentHashing.Hash.find_index(val, n_capacity)
                    if n_ring[idx] is None:
                        n_ring[idx] = val
                    else:
                        v = 0
                        for i in range(idx, n_capacity):
                            if n_ring[i] is None:
                                n_ring[i] = val
                                v += 1

                        if v is n_capacity - 1 and n_ring[v] is not val:
                            for i in range(0, idx):
                                if n_ring[idx] is None:
                                    n_ring[idx] = val

                self.__ring = n_ring
                self.capacity = n_capacity

        def get_server(self, req):
            """
            Return the server instance mapped
            according to the consistent hashing algorithm

            :param req:
            :return ConsistentNode:
            """
            idx = ConsistentHashing.Hash.find_index(req, self.capacity)
            if self.__ring[idx] is not None:
                return self.__ring[idx]
            else:
                for i in range(idx, self.capacity):
                    if self.__ring[idx] is not None:
                        return self.__ring[idx]

                for i in range(0, idx):
                    if self.__ring[idx] is not None:
                        return self.__ring[idx]

                return None

        def update(self, _node, n_node):
            """
            Updates the instance of the
            server based on the consistent
            hashing algorithm.

            :param n_node:
            :param _node:
            :return bool:
            """
            expected_idx = ConsistentHashing.Hash.find_index(_node, self.capacity)
            if (self.__ring[expected_idx] is None or
                    self.__ring[expected_idx] is not _node
                    or n_node is not None):

                for i in range(expected_idx, self.capacity):
                    if self.__ring[i] is _node:
                        self.__ring[i] = n_node
                        return True

                for i in range(0, expected_idx):
                    if self.__ring[i] is _node:
                        self.__ring[i] = n_node
                        return True

            return False

        def delete(self, _node):
            """
            Deletes by updating the
            _node with None in consistent ring

            :param _node:
            :return bool:
            """
            return self.update(_node, None)

        def soft_delete(self, _node: ConsistentNode):
            """
            Soft deletes the node by
            updating the is_down attribute

            :param _node:
            :return bool:
            """

            n_node = ConsistentNode(_node.node)
            n_node.is_down = True
            return self.update(_node, n_node)

    def __init__(self, capacity):
        self.__ring = ConsistentHashing.Ring(capacity)

    def put(self, node: ConsistentNode):
        """
        api for easy calling and
        interacting with the Ring

        :param node:
        :return None:
        """
        if node is not None:
            self.__ring.put_server(node)

    def get_request_server(self, req):
        """
        Returns the server instance
        by consistent hashing algorithm

        :param req:
        :return ConsistentNode:
        """
        if req is not None:
            return self.__ring.get_server(req)

    def get_ring(self):
        """
        To return the Ring instance
        currently used

        :return Ring:
        """
        return self.__ring

    def soft_delete(self, node):
        """
        Soft deletes the instance from the ring

        :param node:
        :return bool:
        """
        if node is not None:
            return self.__ring.soft_delete(node)


class LruCache:
    """ Creates a lru cache implementation """

    def __init__(self, max_size: int, trim_size: int) -> None:
        if max_size <= 0:
            self.__max_size = 1_000
        self.__cache_map = collections.OrderedDict()
        self.__max_size = max_size
        self.trim_size = trim_size
        self.__hit_count = 0
        self.__put_count = 0
        self.__evict_count = 0
        self.__miss_count = 0
        self.__logger = FileLogger("cache.log")

    def trim_to_size(self):
        if self.trim_size <= 0:
            self.trim_size = 100
        current: int = 0
        for key in self.__cache_map.keys() and current != self.trim_size:
            del self.__cache_map[key]
            self.__evict_count += 1
            self.__put_count -= 1
            current += 1
        self.__logger.log(f"Trimmed the cache size till {self.trim_size}")

    def add_val(self, key, value):
        if key is None or value is None:
            raise NullKeyValueException()
        if self.__cache_map.get(key) is not value:
            if len(self.__cache_map) == self.__max_size:
                self.trim_to_size()
            self.__cache_map[key] = value
            self.__put_count += 1
            self.__logger.log(f"The key {key} and value {value} added successfully")
            return True
        else:
            return False

    def update_value(self, key, value):
        if key is None or value is None:
            raise NullKeyValueException()
        if self.__cache_map.get(key) is None:
            self.add_val(key, value)
        else:
            self.__logger.log(f"The key's {key} value {self.__cache_map[key]} is updated with {value}")
            self.__cache_map[key] = value

    def get_value(self, key):
        if key is None:
            raise NullKeyValueException()
        if self.__cache_map.get(key) is None:
            self.__logger.log(f"Read the key {key} successfully")
            self.__miss_count += 1
            return None
        self.__hit_count += 1
        return self.__cache_map[key]


class Bucket:
    """
    Represents a storage bucket with operations like creation, deletion,
    metadata management, and logging.
    """

    def __init__(self, bck_name: str, is_private: bool) -> None:
        if bck_name is None or is_private is None:
            raise NullException()
        self.__bck_name = bck_name
        self.__meta_data = MetaData()
        self.__base_meta_data = {
            "Creation Time": datetime.datetime.now(),
            "Bucket Name": self.__bck_name,
            "Is Private": is_private  # Changed "Bucket Type" to "Is Private"
        }
        self.__meta_data.add_all_meta_data(self.__base_meta_data)
        self.logger = FileLogger(f"Bucket_{self.__bck_name}.log")

    def create_bucket(self):
        """Creates the bucket."""
        try:
            os.makedirs(self.__bck_name)
            self.logger.log(f"Bucket {self.__bck_name} created successfully")
        except FileExistsError:
            self.logger.log(f"Failed to create bucket '{self.__bck_name}': Already exists")

    def delete_bucket(self):
        """Deletes the bucket."""
        if os.path.exists(self.__bck_name):
            shutil.rmtree(self.__bck_name)
            self.logger.log(f"Bucket '{self.__bck_name}' deleted successfully")
        else:
            self.logger.log(f"Failed to delete bucket '{self.__bck_name}': Does not exist")

    def get_meta_data(self):
        """Retrieves metadata associated with the bucket."""
        return self.__meta_data

    def update_meta_data(self, key, value):
        """Updates metadata with the provided key-value pair."""
        try:
            self.__meta_data.update_meta_data(key, value)
            self.logger.log(f"Metadata updated successfully: '{key}': '{value}'")
        except NotFoundException as e:
            self.logger.log(f"Failed to update metadata: {e}")
        except NullKeyValueException as e:
            self.logger.log(f"Failed to update metadata: {e}")

    def delete_meta_data(self, key):
        """Deletes metadata associated with the given key."""
        try:
            self.__meta_data.delete_meta_data(key)
            self.logger.log(f"Metadata '{key}' deleted successfully")
        except NotFoundException as e:
            self.logger.log(f"Failed to delete metadata: {e}")
        except NullKeyValueException as e:
            self.logger.log(f"Failed to delete metadata: {e}")

    def add_all_meta_data(self, meta_data: dict):
        """Adds multiple metadata entries."""
        try:
            self.__meta_data.add_all_meta_data(meta_data)
            self.logger.log("All metadata added successfully")
        except NotFoundException as e:
            self.logger.log(f"Failed to add metadata: {e}")
        except NullKeyValueException as e:
            self.logger.log(f"Failed to add metadata: {e}")

    def add_meta_data(self, key, value):
        """Adds a single metadata entry."""
        try:
            self.__meta_data.add_meta_data(key, value)
            self.logger.log(f"Metadata added successfully: '{key}': '{value}'")
        except NotFoundException as e:
            self.logger.log(f"Failed to add metadata: {e}")
        except NullKeyValueException as e:
            self.logger.log(f"Failed to add metadata: {e}")

    def get_path(self, object_name, object_type):
        return os.path.join(self.__bck_name, f"{object_name}.{object_type}")

    async def upload_object(self, obj):
        """ Handling the upload logic for big data """
        path = self.get_path(obj.get_object_name(), obj.get_object_type())

        # Check if the object already exists
        if os.path.exists(path):
            self.logger.log(f"Object '{obj.get_object_name()}' already exists in the bucket.")
            return

        # If the object doesn't exist, proceed with the upload
        os.makedirs(path, exist_ok=True)  # Use makedirs to create intermediate directories if they don't exist
        meta_data = obj.get_object_meta_data()

        # import json

        # Write metadata to file in JSON format
        with open(os.path.join(path, "meta_data.json"), "w") as meta_file:
            json.dump(meta_data, meta_file)

        async with aiofiles.open(os.path.join(path, str(obj.get_uuid())),
                                 "wb") as data_file:  # Open file for writing in binary mode
            await data_file.write(obj.get_object_data())  # Write data to file

    async def download_object(self, object_name, object_type):
        """Handles the download logic for objects."""
        path = self.get_path(object_name, object_type)
        if not os.path.exists(path):
            raise NotFoundException(
                f"Object '{object_name}' of type '{object_type}' not found in bucket '{self.__bck_name}'")

        object_data = None
        object_meta_data = None

        for root, dirs, files in os.walk(path):
            for file in files:
                if file == "meta_data.json":
                    async with aiofiles.open(os.path.join(root, file), "r") as meta_file:
                        object_meta_data = await meta_file.read()
                else:
                    async with aiofiles.open(os.path.join(root, file), "rb") as data_file:
                        object_data = await data_file.read()

        if object_data is None:
            raise Exception("Failed to read object data")
        if object_meta_data is None:
            raise Exception("Failed to read object metadata")
        meta_data_dict = json.loads(object_meta_data)
        return object_data, meta_data_dict


class FileMimeTypes:
    """
    A small mime db for handling of the different types of files
    """

    def __init__(self) -> None:
        self.mime_db = [
            "image",
            "videos"
        ]


class Object:
    """
    Represents the Object class for storing the data
    The object will itself be a directory and store data
    as a file handles binary data.

    Each object contains following data:
    object_name
    object_type
    object_compressed_flag
    object_id
    object_bucket_name
    object_data
    """

    def __init__(self,
                 object_name,
                 object_bucket_name,
                 object_type,
                 object_data,
                 object_meta_data) -> None:

        self.__mime_db = FileMimeTypes()

        if (object_bucket_name is None or
                object_name is None or
                object_type is None or
                object_data is None or
                object_meta_data is None):
            raise NullException()

        if not os.path.exists(object_bucket_name):
            raise BucketNotFoundException()

        if object_name in os.listdir():
            raise ObjectAlreadyExistsException()

        self.__uuid = uuid.uuid4()
        self.__object_name = object_name
        self.__object_type = object_type
        self.__object_data = object_data
        self.__object_bucket_name = object_bucket_name
        self.__object_meta_data = object_meta_data
        self.__logger = FileLogger(str(self.get_object_name()))
        self.__meta_data_manager = MetaData()

        base_meta_data = {
            "uuid": str(self.__uuid),
            "object name": self.__object_name,
            "bucket_name": self.__object_bucket_name,
            "object_type": self.__object_type,
        }

        self.__meta_data_manager.add_all_meta_data(base_meta_data)
        self.__meta_data_manager.add_all_meta_data(self.__object_meta_data)

    def get_object_name(self):
        return self.__object_name

    def get_object_type(self):
        return self.__object_type

    def get_object_data(self):
        return self.__object_data

    def get_object_bucket_name(self):
        return self.__object_bucket_name

    def get_object_meta_data(self):
        return self.__meta_data_manager.get_all_meta_data()

    def get_uuid(self):
        return self.__uuid

    # def encode_object_data(self):
    #     encoder = ReedSolomonEncoder(data_shards=6, parity_shards=3)  # Example parameters, adjust as needed
    #     self.__object_data = encoder.encode(self.__object_data)
    #
    # def decode_object_data(self):
    #     encoder = ReedSolomonEncoder(data_shards=6, parity_shards=3)  # Example parameters, adjust as needed
    #     self.__object_data = encoder.decode(self.__object_data)


app = Flask(__name__)

# Create a bucket
bucket_name = "test_bucket"
bucket = Bucket(bucket_name, is_private=False)
bucket.create_bucket()


# Endpoint for uploading an object
@app.route('/upload/<object_name>', methods=['POST'])
def upload_object(object_name):
    """
    Takes in a file and name and uploads to the storage bucket

    :param  object_name:
    :return json-resp and status code:
    """

    # Get object data from request
    object_data = request.files['object_data'].read()
    index = object_name.index(".")
    object_type = str(object_name)[index + 1:]
    _object_name = str(object_name)[:index]
    object_meta_data = {"type": object_type}

    # Create and encode the object
    obj = Object(_object_name, bucket_name, object_type, object_data, object_meta_data)
    # obj.encode_object_data()  # Encode object data using Reed-Solomon
    asyncio.run(bucket.upload_object(obj))

    return jsonify({"message": "Object uploaded successfully"}), 200


# Modify the download endpoint to decode object data after downloading
@app.route('/download/<object_name>/<object_t>/<object_type>', methods=['GET'])
def download_object(object_t, object_name, object_type):
    """
    End Point for downloading the object from the bucket
    This function returns the metadata as a response header parameter
    in json form

    :param object_t:
    :param object_name:
    :param object_type:
    :return metadata and object_data:
    """
    try:
        # Download the object
        object_data, meta_data = asyncio.run(bucket.download_object(object_name, object_type))

        # Decode object data using Reed-Solomon
        obj = Object(object_name, bucket_name, object_type, object_data, meta_data)
        # obj.decode_object_data()

        # Create a response
        object_extension = meta_data["type"]
        response = make_response(send_file(
            io.BytesIO(obj.get_object_data()),
            mimetype=f'{object_t}/{object_extension}',  # Adjust the mimetype as per your image type
        ))

        # Include metadata as custom headers
        response.headers['X-Metadata'] = json.dumps(meta_data)

        return response
    except NotFoundException as e:
        return jsonify({"error": str(e)}), 404


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
