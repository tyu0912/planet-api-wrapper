import boto3


def upload_to_aws(local_file, s3_bucket, s3_file):
    s3 = boto3.client('s3')


    try:
        s3.upload_file(local_file, s3_bucket, s3_file)
        print("Upload Successful")
        return True

    except FileNotFoundError:
        print("The file was not found")
        return False
    # except NoCredentialsError:
    #     print("Credentials not available")
    #     return False

# Function to create the s3 key for the image of a given waypoint
def get_s3_key_for_image(waypoint_row, extension='tif'):
    lat_lon_name = waypoint_row['lat_lon_name']
    image_id = waypoint_row['Image_ID']

    path = f"planet_images/test_conor/{lat_lon_name}/{image_id}_3B_AnalyticMS_SR_clip.{extension}"
    return path


def s3_object_exists(Bucket, Key):
    try:
        s3_client = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY,
                                 aws_secret_access_key=S3_SECRET_KEY)
        s3_client.head_object(Bucket=Bucket,
                              Key=Key)
        return True
    except:
        return False