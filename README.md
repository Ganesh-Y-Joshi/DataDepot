# DataDepot
Currently Developing Stage
Object Storage API
This API provides functionality for storing and retrieving objects in a storage bucket. It supports uploading and downloading objects, along with metadata management.

Endpoints
1. Upload Object
URL: /upload/<object_name>
Method: POST
Description: Uploads an object to the storage bucket.
Request Body: Form-data with the key object_data containing the object file.
Response: JSON response indicating success or failure.
2. Download Object
URL: /download/<object_name>/<object_t>/<object_type>
Method: GET
Description: Downloads an object from the storage bucket.
Parameters:
object_name: Name of the object to download.
object_t: Type of the object (e.g., "Image", "Video").
object_type: File extension/type of the object.
Response: The object data along with metadata included in the response headers (X-Metadata).
Metadata Management
Metadata associated with objects can be managed using the following operations:
Adding metadata
Updating metadata
Deleting metadata
Logging
Logging functionality is implemented to track operations performed on buckets and objects.
Error Handling
Proper error handling is implemented to handle cases such as object not found, bucket not found, etc.
Future Improvements
Implementing Reed-Solomon encoding/decoding for object data.
Enhancing security features such as access control and encryption.
Adding support for additional storage functionalities like object deletion and listing.
Implementing caching mechanisms for improved performance.



This API is currently in development, and further enhancements and improvements are planned.





