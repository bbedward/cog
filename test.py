import time
from PIL import Image
from io import BytesIO
from typing import Iterable, List, Optional
from concurrent.futures import Future, ThreadPoolExecutor


class UploadObject:
    def __init__(
        self,
        img_bytes: bytes,
        target_extension: str,
        target_quality: int,
    ):
        self.img_bytes = img_bytes
        self.target_extension = target_extension
        self.target_quality = target_quality


def upload_to_s3(
    img_bytes: bytes,
    target_quality: int,
    target_extension: str,
    upload_path_prefix: str,
) -> str:
    start_conv = time.time()
    img_format = target_extension[1:].upper()
    mode = "RGBA"
    if target_extension == ".jpeg":
        mode = "RGB"
    pil_image = Image.frombytes(mode, (1248, 344), img_bytes)
    converted_bytes = BytesIO()
    pil_image.save(converted_bytes, format=img_format, quality=target_quality)
    file_bytes = converted_bytes.getvalue()
    end_conv = time.time()
    print(
        f"Converted image in: {round((end_conv - start_conv) *1000)} ms - {img_format} - {target_quality}"
    )
    key = f"asdf{target_extension}"
    if upload_path_prefix is not None and upload_path_prefix != "":
        key = f"{upload_path_prefix}{key}"

    start_upload = time.time()
    print(f"Uploading image to: {key}")
    end_upload = time.time()
    print(f"Uploaded image in: {round((end_upload - start_upload) *1000)} ms")

    return f"s3://asdf/{key}"


def upload_files(
    uploadObjects: List[UploadObject], upload_path_prefix: str
) -> Iterable[str]:
    """Upload all files to S3 in parallel and return the S3 URLs"""
    start = time.time()
    # Run all uploads at same time in threadpool
    tasks: List[Future] = []
    with ThreadPoolExecutor(max_workers=len(uploadObjects)) as executor:
        for uo in uploadObjects:
            tasks.append(
                executor.submit(
                    upload_to_s3,
                    uo.img_bytes,
                    uo.target_quality,
                    uo.target_extension,
                    upload_path_prefix,
                )
            )

    # Get results
    results = []
    for task in tasks:
        results.append(task.result())

    end = time.time()
    print(f"📤 All converted and uploaded to S3 in: {round((end - start) *1000)} ms 📤")

    return results


img = Image.open("test.png")
start_conv = time.time()
img_bytes = img.tobytes()
end_conv = time.time()
print(f"Converted image in: {round((end_conv - start_conv) *1000)} ms")

upload_objects = [
    UploadObject(img_bytes, ".jpeg", 100),
    UploadObject(img_bytes, ".jpeg", 100),
]

upload_files(upload_objects, "asdf")
