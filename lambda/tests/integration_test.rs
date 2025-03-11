use assert_fs::TempDir;
use aws_lambda_events::sqs::SqsMessage;
use lambda::process_record;
use std::env::set_var;
use std::fs::{read, write};
use std::path::PathBuf;
use testlib::*;
use tokio;
use wiremock::http::Method;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn downloads_the_live_package_uploads_anonymised_package_send_to_queue() {
    let input_dir: TempDir = TempDir::new().unwrap();
    let tar_path = create_package(&input_dir, valid_json(), None);
    let test_input_bucket = "test-input-bucket";
    let test_output_bucket = "test-output-bucket";
    set_var("OUTPUT_BUCKET", test_output_bucket);
    set_var("OUTPUT_QUEUE", "https://example.com");

    let test_download_key = tar_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap();
    let test_upload_key = test_download_key.replace("TDR", "TST");
    let test_string = format!(
        r#"{{"parameters": {{"status":"ok","reference":"test-reference", "s3Bucket": "{test_input_bucket}", "s3Key": "{test_download_key}"}}}}"#,
    );
    let message = SqsMessage {
        body: Some(test_string),
        ..Default::default()
    };

    let get_object_path = format!("/{test_input_bucket}/{test_download_key}");
    let put_object_path = format!("/{test_output_bucket}/{test_upload_key}");
    let mock_s3_server = MockServer::start().await;
    let mock_sqs_server = MockServer::start().await;
    let bytes = read(tar_path).unwrap();

    Mock::given(method("GET"))
        .and(path(get_object_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(&mock_s3_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(put_object_path))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_s3_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_sqs_server)
        .await;
    let s3_uri = mock_s3_server.uri();
    let sqs_uri = mock_sqs_server.uri();
    let _ = process_record(
        &message,
        input_dir.to_owned(),
        Some(s3_uri.as_str()),
        Some(sqs_uri.as_str()),
    )
    .await
    .unwrap();

    let s3_requests = &mock_s3_server.received_requests().await.unwrap();
    let put_request = s3_requests
        .iter()
        .filter(|req| req.method == Method::Put)
        .last()
        .unwrap();

    let sqs_requests = &mock_sqs_server.received_requests().await.unwrap();
    let send_message_request = sqs_requests.last().unwrap();
    let body = &send_message_request.body;
    let sqs_message_string = String::from_utf8(body.to_vec()).unwrap();
    let expected_string = r#"{"QueueUrl":"https://example.com","MessageBody":"{\"parameters\":{\"status\":\"ok\",\"reference\":\"test-reference\",\"s3Bucket\":\"test-output-bucket\",\"s3Key\":\"TST-2023.tar.gz\"}}"}"#;
    assert_eq!(sqs_message_string, expected_string);

    let path_to_output_file = input_dir.to_owned().join("output.tar.gz");
    let output_dir = TempDir::new().unwrap();
    write(&path_to_output_file, &put_request.body[5..]).unwrap();
    decompress_test_file(&path_to_output_file, &output_dir);
    let metadata_json = get_metadata_json_fields(&output_dir.to_owned());
    assert_eq!(metadata_json.contact_email, "XXXXXXXXX");
    assert_eq!(metadata_json.contact_name, "XXXXXXXXX");
    assert_eq!(
        metadata_json.checksum,
        "9330f5cb8b67a81d3bfdedc5b9f5b84952a2c0d2f76a3208b84901febdf4db6a"
    );
}

#[tokio::test]
async fn error_if_key_is_missing_from_bucket() {
    let test_input_bucket = "test-input-bucket";
    let test_output_bucket = "test-output-bucket";
    set_var("OUTPUT_BUCKET", test_output_bucket);
    set_var("OUTPUT_QUEUE", "https://example.com");

    let test_download_key = "missing-key.tar.gz";
    let get_object_path = format!("/{test_input_bucket}/{test_download_key}");
    let mock_s3_server = MockServer::start().await;
    let mock_sqs_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(get_object_path))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_s3_server)
        .await;
    let test_string = format!(
        r#"{{"parameters": {{"status":"ok","reference":"test-reference", "s3Bucket": "{test_input_bucket}", "s3Key": "{test_download_key}"}}}}"#
    );
    let message = SqsMessage {
        body: Some(test_string),
        ..Default::default()
    };
    let err = process_record(
        &message,
        PathBuf::from("/tmp"),
        Some(mock_s3_server.uri().as_str()),
        Some(mock_sqs_server.uri().as_str()),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "service error")
}

#[tokio::test]
async fn error_if_key_is_not_a_tar_file() {
    let test_input_bucket = "test-input-bucket";
    let test_output_bucket = "test-output-bucket";
    set_var("OUTPUT_BUCKET", test_output_bucket);
    set_var("OUTPUT_QUEUE", "https://example.com");

    let test_download_key = "test.tar.gz";
    let get_object_path = format!("/{test_input_bucket}/{test_download_key}");
    let mock_s3_server = MockServer::start().await;
    let mock_sqs_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(get_object_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes("test".as_bytes()))
        .mount(&mock_s3_server)
        .await;
    let test_string = format!(
        r#"{{"parameters": {{"status":"ok","reference":"test-reference", "s3Bucket": "{test_input_bucket}", "s3Key": "{test_download_key}"}}}}"#
    );
    let message = SqsMessage {
        body: Some(test_string),
        ..Default::default()
    };
    let err = process_record(
        &message,
        PathBuf::from("/tmp"),
        Some(mock_s3_server.uri().as_str()),
        Some(mock_sqs_server.uri().as_str()),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "failed to iterate over archive")
}

#[tokio::test]
async fn error_if_upload_fails() {
    let input_dir: TempDir = TempDir::new().unwrap();
    let tar_path = create_package(&input_dir, valid_json(), None);
    let test_input_bucket = "test-input-bucket";
    let test_output_bucket = "test-output-bucket";
    set_var("OUTPUT_BUCKET", test_output_bucket);
    set_var("OUTPUT_QUEUE", "https://example.com");

    let test_download_key = tar_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap();
    let test_upload_key = test_download_key.replace("TDR", "TST");
    let test_string = format!(
        r#"{{"parameters": {{"status":"ok","reference":"test-reference", "s3Bucket": "{test_input_bucket}", "s3Key": "{test_download_key}"}}}}"#
    );
    let message = SqsMessage {
        body: Some(test_string),
        ..Default::default()
    };

    let get_object_path = format!("/{test_input_bucket}/{test_download_key}");
    let put_object_path = format!("/{test_input_bucket}/{test_upload_key}");
    let mock_s3_server = MockServer::start().await;
    let mock_sqs_server = MockServer::start().await;
    let bytes = read(tar_path).unwrap();

    Mock::given(method("GET"))
        .and(path(get_object_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(&mock_s3_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(put_object_path))
        .respond_with(ResponseTemplate::new(401))
        .mount(&mock_s3_server)
        .await;
    let err = process_record(
        &message,
        input_dir.to_owned(),
        Some(mock_s3_server.uri().as_str()),
        Some(mock_sqs_server.uri().as_str()),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "service error")
}

#[tokio::test]
async fn error_for_invalid_input() {
    let test_string_missing_bucket =
        r#"{"parameters": {"status":"ok","reference":"test-reference", "s3Key": "key"}}"#;
    let test_string_missing_key =
        r#"{"parameters": {"status":"ok","reference":"test-reference", "s3Bucket": "bucket"}}"#;
    let missing_body_message = SqsMessage::default();
    let missing_bucket_message = SqsMessage {
        body: Some(test_string_missing_bucket.to_string()),
        ..Default::default()
    };
    let missing_key_message = SqsMessage {
        body: Some(test_string_missing_key.to_string()),
        ..Default::default()
    };
    let uri = Some("https://example.com");
    let missing_body_err = process_record(
        &missing_body_message,
        TempDir::new().unwrap().to_owned(),
        uri,
        uri,
    )
    .await
    .unwrap_err();
    let missing_bucket_err = process_record(
        &missing_bucket_message,
        TempDir::new().unwrap().to_owned(),
        uri,
        uri,
    )
    .await
    .unwrap_err();
    let missing_key_err = process_record(
        &missing_key_message,
        TempDir::new().unwrap().to_owned(),
        uri,
        uri,
    )
    .await
    .unwrap_err();

    assert_eq!(
        missing_body_err.to_string(),
        "No body found in the SQS message"
    );
    assert_eq!(
        missing_bucket_err.to_string(),
        "missing field `s3Bucket` at line 1 column 75"
    );
    assert_eq!(
        missing_key_err.to_string(),
        "missing field `s3Key` at line 1 column 81"
    );
}
