import hashlib

def calculate_file_hash(file_path):
    """
    Calculate the SHA-256 hash of a file.

    Args:
    - file_path (str): The path to the file.

    Returns:
    - str: The SHA-256 hash value of the file.
    """
    # Open the file in binary mode and read its content
    with open(file_path, "rb") as file:
        # Calculate the SHA-256 hash of the file content
        hash_object = hashlib.sha256()
        while True:
            # Read data from the file in chunks
            data = file.read(4096)
            if not data:
                break
            # Update the hash object with the data
            hash_object.update(data)

    # Return the hexadecimal representation of the hash
    return hash_object.hexdigest()

def check_chunk_integrity(chunk_data, file_path, chunk_offset, chunk_length):
    """
    Check the integrity of a chunk of data by comparing its SHA-256 hash
    with the hash of the corresponding chunk from the original file.

    Args:
    - chunk_data (bytes): The data of the chunk to be checked.
    - file_path (str): The path to the original file.
    - chunk_offset (int): The offset of the chunk within the original file.
    - chunk_length (int): The length of the chunk.

    Returns:
    - bool: True if the hash of the chunk matches the hash of the corresponding
            chunk from the original file, False otherwise.
    """
    # Calculate the SHA-256 hash of the chunk data
    calculated_hash = hashlib.sha256(chunk_data).hexdigest()

    # Open the original file and seek to the offset of the chunk
    with open(file_path, "rb") as file:
        file.seek(chunk_offset)
        # Read the corresponding chunk from the original file
        original_chunk_data = file.read(chunk_length)

    # Calculate the SHA-256 hash of the original chunk
    original_hash = hashlib.sha256(original_chunk_data).hexdigest()

    # Compare the calculated hash with the original hash
    if calculated_hash == original_hash:
        return True
    else:
        return False
