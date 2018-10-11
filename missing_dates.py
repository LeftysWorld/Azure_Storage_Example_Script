from azure.storage.blob import BlockBlobService

from config import (
    STORAGE_ACCOUNT_NAME,
    ACCESS_KEY,
    CONTAINER_NAME,
    START_DATE
)


def main():
    block_blob_service = BlockBlobService(account_name=STORAGE_ACCOUNT_NAME, account_key=ACCESS_KEY)
    generator = block_blob_service.list_blobs(CONTAINER_NAME)
    needed_blob_dr = "needed_blob_dump/"

    try:
        for blob in generator:
            blob_name, blob_type = blob.name.split(".")
            blob_year, blob_month = blob_name.split("/")
            full_blob_path = "{0}{1}".format(needed_blob_dr, blob.name)

            if str(blob.name) == '{}.zip'.format(START_DATE):
                print("START BLOB")
                block_blob_service.get_blob_to_path(CONTAINER_NAME, blob.name, full_blob_path)
            print("")
        print("FINISHED ALL")

    except Exception as e:
        print("ERROR: ", e)


if __name__ == '__main__':
    main()
