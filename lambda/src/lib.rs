//! Anonymiser lambda
//!
//! This lambda is used to convert incoming scripts from the TRE production bucket into anonymised packages
//!
//! Given the following input:
//! ```json
//! {
//!   "parameters": {
//!     "s3Bucket": "input-bucket",
//!     "s3Key": "TRE-TDR-2023-ABC.tar.gz"
//!   }
//! }
//! ```
//! The lambda will:
//! * Download the file from S3 to local disk
//! * Anonymise it using the anonymise library
//! * Upload it to S3 using the `OUTPUT_BUCKET` environment variable
//! * Send the SQS message to the queue specified in the `OUTPUT_QUEUE` environment variable

use anonymiser_lib::process_package;
use aws_config::meta::region::RegionProviderChain;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_lambda_events::sqs::SqsMessage;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use lambda_runtime::Error;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The bucket and key for the file we are processing
#[derive(Deserialize, Serialize)]
struct MessageBody {
    parameters: S3Details,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct S3Details {
    status: String,
    reference: String,
    s3_bucket: String,
    s3_key: String,
}

/// # Processes the SQS message.
///
/// This will download the file specified in the message body, anonymise it, upload it to S3 and send the message on to the output queue.
pub async fn process_record(
    message: &SqsMessage,
    working_directory: PathBuf,
    s3_endpoint_url: Option<&str>,
    sqs_endpoint_url: Option<&str>,
) -> Result<PathBuf, Error> {
    let body = message
        .body
        .as_ref()
        .ok_or("No body found in the SQS message")?;
    let s3_client = create_s3_client(s3_endpoint_url).await;
    let sqs_client = create_sqs_client(sqs_endpoint_url).await;

    let message_body: MessageBody = serde_json::from_str(body)?;
    let parameters = message_body.parameters;
    let input_file_path = download(
        &s3_client,
        parameters.s3_bucket,
        parameters.s3_key,
        &working_directory,
    )
    .await?;
    let output_path = &working_directory.join(PathBuf::from("output"));
    fs::create_dir_all(output_path)?;
    let output_tar_path = process_package(output_path, &input_file_path)?;
    let file_name = output_tar_path
        .file_name()
        .and_then(|file_name_as_os_string| file_name_as_os_string.to_str())
        .expect("Cannot parse file name from output path");

    let output_bucket = std::env::var("OUTPUT_BUCKET")?;
    upload(&s3_client, &output_tar_path, &output_bucket, file_name).await?;

    let output_queue = std::env::var("OUTPUT_QUEUE")?;
    let reference = parameters.reference.replace("TDR", "TST");
    let status = parameters.status;
    let output_message_body = MessageBody {
        parameters: S3Details {
            s3_bucket: output_bucket,
            s3_key: file_name.to_string(),
            status,
            reference,
        },
    };
    let message_string = serde_json::to_string(&output_message_body)?;
    let _ = sqs_client
        .send_message()
        .queue_url(&output_queue)
        .message_body(message_string)
        .send()
        .await?;
    Ok(output_path.clone())
}

/// # Uploads the specified file
///
/// This will upload the contents of the file in `body_path` to the `bucket` with the specified `key`
async fn upload(
    client: &S3Client,
    body_path: &PathBuf,
    bucket: &str,
    key: &str,
) -> Result<(), Error> {
    let body = ByteStream::from_path(body_path).await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(())
}

/// # Downloads the specified file
///
/// This downloads the contents of the file in the S3 `bucket` with the specified `key` into the `working_directory`
async fn download(
    client: &S3Client,
    bucket: String,
    key: String,
    working_directory: &Path,
) -> Result<PathBuf, Error> {
    let destination = working_directory.join(PathBuf::from(&key));
    let mut destination_path = destination.clone();
    destination_path.pop();
    fs::create_dir_all(&destination_path)?;

    let mut file = File::create(&destination)?;

    let mut object = client.get_object().bucket(bucket).key(&key).send().await?;

    while let Some(bytes) = object.body.try_next().await? {
        file.write_all(&bytes)?;
    }

    Ok(destination)
}

/// # Creates an SQS client
async fn create_sqs_client(potential_endpoint_url: Option<&str>) -> SQSClient {
    let config = aws_config("sqs", potential_endpoint_url).await;
    SQSClient::new(&config)
}

/// # Creates an AWS SDK config object
async fn aws_config(service: &str, potential_endpoint_url: Option<&str>) -> SdkConfig {
    let default_endpoint = format!("https://{service}.eu-west-2.amazonaws.com");
    let endpoint_url = potential_endpoint_url.unwrap_or(default_endpoint.as_str());
    let region_provider = RegionProviderChain::default_provider().or_else("eu-west-2");

    aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint_url)
        .load()
        .await
}

/// # Creates an S3 client
async fn create_s3_client(potential_endpoint_url: Option<&str>) -> S3Client {
    let config = aws_config("s3", potential_endpoint_url).await;
    S3Client::new(&config)
}

#[cfg(test)]
mod test {
    use crate::{aws_config, create_s3_client};

    #[tokio::test]
    async fn test_create_client_with_default_region() {
        let client = create_s3_client(None).await;
        let config = client.config();

        assert_eq!(config.region().unwrap().to_string(), "eu-west-2");
    }

    #[tokio::test]
    async fn test_aws_config_endpoint_url() {
        let config_default_endpoint = aws_config("test", None).await;
        assert_eq!(
            config_default_endpoint.endpoint_url().unwrap(),
            "https://test.eu-west-2.amazonaws.com"
        );

        let config_custom_endpoint = aws_config("test", Some("https://example.com")).await;
        assert_eq!(
            config_custom_endpoint.endpoint_url().unwrap(),
            "https://example.com"
        );
    }
}
